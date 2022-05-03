package raftstore

import (
	"github.com/golang/protobuf/proto"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
)

func (d *peerMsgHandler) applyChangesToBadger(kvWb *engine_util.WriteBatch, index uint64) *engine_util.WriteBatch {
	d.persistApplyState(kvWb, index)
	d.writeChangesToKVDB(kvWb)
	return kvWb
}

func (d *peerMsgHandler) persistApplyState(kvWb *engine_util.WriteBatch, index uint64) {
	d.peerStorage.applyState.AppliedIndex = index
	err := kvWb.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
	if err != nil {
		log.Errorf("[region %d] node %d persist apply state to %d failed, err %v",
			d.regionId, d.RaftGroup.Raft.ID(), index, err)
	}
}

func (d *peerMsgHandler) writeChangesToKVDB(kvWb *engine_util.WriteBatch) {
	if err := kvWb.WriteToDB(d.peerStorage.Engines.Kv); err != nil {
		log.Errorf("[region %d] node %d write changes to kvdb failed, err %v",
			d.regionId, d.RaftGroup.Raft.ID(), err)
	}
	kvWb.Reset()
}

func (d *peerMsgHandler) unmarshalEntry(entry eraftpb.Entry) (raft_cmdpb.RaftCmdRequest, error) {
	msg := raft_cmdpb.RaftCmdRequest{}
	var data []byte
	// unmarshal entry of config change
	if entry.EntryType == eraftpb.EntryType_EntryConfChange {
		cc := eraftpb.ConfChange{}
		if err := cc.Unmarshal(entry.Data); err != nil {
			log.Errorf("[region %d] node %d unmarshal confChange entry failed, err %v",
				d.regionId, d.RaftGroup.Raft.ID(), err)
			return msg, err
		}
		data = cc.Context
	} else {
		data = entry.Data
	}
	err := msg.Unmarshal(data)
	if err != nil {
		log.Errorf("[region %d] node %d unmarshal msg failed, err %v",
			d.regionId, d.RaftGroup.Raft.ID(), err)
		return msg, err
	}
	return msg, nil
}

func (d *peerMsgHandler) applyCommittedEntry(entry eraftpb.Entry, kvWb *engine_util.WriteBatch) {
	if d.stopped || entry.Data == nil {
		return
	}
	cb := d.getCallbackFromProposals(entry.Index, entry.Term)
	msg, err := d.unmarshalEntry(entry)
	if err != nil {
		cb.Done(ErrResp(err))
		return
	}

	// 非 Admin Request， Proposal 中的 version 与当前的不相等。
	// Split，Merge 的 Request，Proposal 中的 Region epoch 与当前的不相等。
	// check region version
	if msg.Header.RegionEpoch != nil && msg.Header.RegionEpoch.Version != d.Region().RegionEpoch.Version {
		cb.Done(ErrResp(&util.ErrEpochNotMatch{}))
		return
	}

	// can't enclose normal requests and administrator request at same time.
	if msg.AdminRequest != nil {
		d.applyAdminCmd(kvWb, msg, cb)
	} else if len(msg.Requests) > 0 {
		d.applyMultiDataCmd(kvWb, msg.Requests, entry, cb)
	}
	return
}

func (d *peerMsgHandler) applyMultiDataCmd(kvWb *engine_util.WriteBatch, requests []*raft_cmdpb.Request, entry eraftpb.Entry, cb *message.Callback) {
	if cb == nil {
		if d.RaftGroup.Raft.IsLeader() {
			log.Debugf("[region %d] node %d ld:%d term:%d cannot find cb for i:%d,t:%d",
				d.regionId, d.RaftGroup.Raft.ID(), d.RaftGroup.Raft.Lead, d.RaftGroup.Raft.Term, entry.Index, entry.Term)
			return
		} else {
			// empty callback ensure follower can apply committed entries.
			cb = message.NewCallback()
		}
	}

	resps := make([]*raft_cmdpb.Response, 0, len(requests))
	for _, req := range requests {
		resp, ok := d.applyDataCmd(kvWb, entry.Index, req, cb)
		if !ok {
			log.Errorf("[region %d] node %d apply raft cmd i:%d,t:%d failed, err: %v, %+v",
				d.regionId, d.RaftGroup.Raft.ID(), entry.Index, entry.Term, cb.Resp, req)
			return
		}
		resps = append(resps, resp)
	}
	cbResp := newCmdResp()
	cbResp.Responses = resps
	log.Debugf("[region %d] node %d apply raft cmd success ld:%d term:%d, i:%d,t:%d, reqs:%+v",
		d.regionId, d.RaftGroup.Raft.ID(), d.RaftGroup.Raft.Lead, d.RaftGroup.Raft.Term, entry.Index, entry.Term, requests)
	cb.Done(cbResp)
	return
}

func (d *peerMsgHandler) applyDataCmd(kvWb *engine_util.WriteBatch, entryIndex uint64, req *raft_cmdpb.Request, cb *message.Callback) (*raft_cmdpb.Response, bool) {
	var key []byte

	switch req.CmdType {
	case raft_cmdpb.CmdType_Get:
		key = req.Get.Key
	case raft_cmdpb.CmdType_Put:
		key = req.Put.Key
	case raft_cmdpb.CmdType_Delete:
		key = req.Delete.Key
	}
	if key != nil {
		if err := util.CheckKeyInRegion(key, d.Region()); err != nil {
			cb.Done(ErrResp(err))
			return nil, false
		}
	}

	resp := &raft_cmdpb.Response{}
	switch req.CmdType {
	case raft_cmdpb.CmdType_Get:
		{
			// apply write changes before read, update applied index to current index
			d.applyChangesToBadger(kvWb, entryIndex)
			val, err := engine_util.GetCF(d.peerStorage.Engines.Kv, req.Get.Cf, req.Get.Key)
			if err != nil {
				cb.Done(ErrResp(err))
				return nil, false
			}

			resp.CmdType = raft_cmdpb.CmdType_Get
			resp.Get = &raft_cmdpb.GetResponse{Value: val}
		}
	case raft_cmdpb.CmdType_Snap:
		{
			d.applyChangesToBadger(kvWb, entryIndex)
			cb.Txn = d.peerStorage.Engines.Kv.NewTransaction(false)
			resp.CmdType = raft_cmdpb.CmdType_Snap
			resp.Snap = &raft_cmdpb.SnapResponse{
				Region: d.Region(),
			}
		}
	case raft_cmdpb.CmdType_Put:
		{
			kvWb.SetCF(req.Put.Cf, req.Put.Key, req.Put.Value)
			resp.CmdType = raft_cmdpb.CmdType_Put
			resp.Put = &raft_cmdpb.PutResponse{}
		}
	case raft_cmdpb.CmdType_Delete:
		{
			kvWb.DeleteCF(req.Delete.Cf, req.Delete.Key)
			resp.CmdType = raft_cmdpb.CmdType_Delete
			resp.Delete = &raft_cmdpb.DeleteResponse{}
		}
	}
	// cb done outside
	return resp, true
}

func (d *peerMsgHandler) applyAdminCmd(kvWb *engine_util.WriteBatch, msg raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	req := msg.AdminRequest
	if req.CmdType != raft_cmdpb.AdminCmdType_CompactLog {
		if err := util.CheckRegionEpoch(&msg, d.Region(), true); err != nil {
			log.Debugf("[region %d] node %d ld:%d term:%d cannot apply admin cmd %v, err: %v",
				d.regionId, d.RaftGroup.Raft.ID(), d.RaftGroup.Raft.Lead, d.RaftGroup.Raft.Term, req, err)
			cb.Done(ErrResp(err))
			return
		}
	}
	if err := util.CheckRegionEpoch(&msg, d.Region(), true); err != nil {
		cb.Done(ErrResp(err))
		return
	}
	switch req.CmdType {
	case raft_cmdpb.AdminCmdType_CompactLog:
		d.applyCompactLog(kvWb, req.CompactLog)
	case raft_cmdpb.AdminCmdType_ChangePeer:
		d.applyChangePeer(kvWb, req.ChangePeer, cb)
	case raft_cmdpb.AdminCmdType_Split:
		d.applySplit(kvWb, req.Split, cb)
	}
}

func (d *peerMsgHandler) applyCompactLog(kvWb *engine_util.WriteBatch, req *raft_cmdpb.CompactLogRequest) {
	if req.CompactIndex > d.peerStorage.applyState.TruncatedState.Index {
		d.peerStorage.applyState.TruncatedState.Index = req.CompactIndex
		d.peerStorage.applyState.TruncatedState.Term = req.CompactTerm

		d.persistApplyState(kvWb, d.peerStorage.applyState.AppliedIndex)
	}
	d.ScheduleCompactLog(req.CompactIndex)
}

func (d *peerMsgHandler) applyChangePeer(kvWb *engine_util.WriteBatch, req *raft_cmdpb.ChangePeerRequest, cb *message.Callback) {
	region := d.Region()
	existed := false
	for _, p := range region.Peers {
		if p.Id == req.Peer.Id {
			existed = true
			break
		}
	}

	switch req.ChangeType {
	case eraftpb.ConfChangeType_AddNode:
		if existed {
			return
		}

		d.ctx.storeMeta.changeRegionPeer(region, req.Peer, true)
		d.insertPeerCache(req.Peer)
	case eraftpb.ConfChangeType_RemoveNode:
		if !existed {
			return
		}
		if len(d.RaftGroup.Raft.Prs) < 3 {
			log.Warnf("[region %d] node %d try to remove %d", d.regionId, d.RaftGroup.Raft.ID(), req.Peer.Id)
		}
		d.ctx.storeMeta.changeRegionPeer(region, req.Peer, false)
		d.removePeerCache(req.Peer.Id)
	}
	if d.IsLeader() {
		d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
	}
	peerState := raft_serverpb.PeerState_Normal

	// destory self to prevent removed node start election
	if req.Peer.Id == d.Meta.Id && req.ChangeType == eraftpb.ConfChangeType_RemoveNode {
		log.Infof("[region %d] node %d metaId %d start destory self %+v",
			d.regionId, d.RaftGroup.Raft.ID(), d.Meta.Id, req)
		peerState = raft_serverpb.PeerState_Tombstone
		d.destroyPeer()
	}
	// it will apply to bager after desctoryPeer(),
	// so we need to ensure peerState consistent with destoryPeer()
	// actullay, we dont apply to bager after region had been destory
	meta.WriteRegionState(kvWb, region, peerState)

	d.RaftGroup.ApplyConfChange(eraftpb.ConfChange{
		ChangeType: req.ChangeType,
		NodeId:     req.Peer.Id,
	})
	log.Infof("[region %d] node %d metaId %d applied conf change %+v",
		d.regionId, d.RaftGroup.Raft.ID(), d.Meta.Id, req)
}

/*
配置变更的时候， conf_ver+ 1。
Split 的时候，原 region 与新 region 的 version均等于原 region 的 version+ 新 region 个数。
Merge 的时候，两个 region 的 version均等于这两个 region 的 version最大值 + 1。
*/
func (d *peerMsgHandler) applySplit(kvWb *engine_util.WriteBatch, req *raft_cmdpb.SplitRequest, cb *message.Callback) {

	region := d.Region()

	if err := util.CheckKeyInRegion(req.SplitKey, region); err != nil {
		cb.Done(ErrResp(err))
		return
	}

	// split region
	originRegion := proto.Clone(d.Region()).(*metapb.Region)
	originRegion.RegionEpoch.Version++
	newRegion := util.CopyRegion(originRegion, req)
	originRegion.EndKey = req.SplitKey

	// log.Infof("[region %d] node %d apply split to [%s,%s,%s],\noldRegionId %+v,\nnewRegionId %+v",
	// 	d.Region().Id, d.RaftGroup.Raft.ID(),
	// 	d.Region().StartKey, req.SplitKey, d.Region().EndKey,
	// 	originRegion, newRegion)

	// update global meta
	d.ctx.storeMeta.split(d.Region(), originRegion, newRegion)
	d.peerStorage.SetRegion(originRegion)

	// create new peer
	newPeer, err := createPeer(d.ctx.store.Id, d.ctx.cfg, d.ctx.regionTaskSender, d.ctx.engine, newRegion)
	if err != nil {
		cb.Done(ErrResp(err))
		return
	}
	for _, p := range newRegion.Peers {
		newPeer.insertPeerCache(p)
	}
	d.ctx.router.register(newPeer)
	_ = d.ctx.router.send(newRegion.Id, message.Msg{Type: message.MsgTypeStart})

	// write region state
	meta.WriteRegionState(kvWb, originRegion, raft_serverpb.PeerState_Normal)
	meta.WriteRegionState(kvWb, newRegion, raft_serverpb.PeerState_Normal)
	d.SizeDiffHint = 0
	d.ApproximateSize = new(uint64)

	resp := newCmdResp()
	resp.AdminResponse = &raft_cmdpb.AdminResponse{
		CmdType: raft_cmdpb.AdminCmdType_Split,
		Split: &raft_cmdpb.SplitResponse{
			Regions: []*metapb.Region{originRegion, newRegion},
		},
	}
	cb.Done(resp)

	if d.IsLeader() {
		d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
	}
	newPeer.HeartbeatScheduler(d.ctx.schedulerTaskSender)
}
