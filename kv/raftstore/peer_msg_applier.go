package raftstore

import (
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
)

func (d *peerMsgHandler) applyChangesToBadger(kvWb *engine_util.WriteBatch, index uint64) *engine_util.WriteBatch {
	d.persistApplyState(kvWb, index)
	d.writeChangesToKVDB(kvWb)
	kvWb.Reset()
	return kvWb
}

func (d *peerMsgHandler) persistApplyState(kvWb *engine_util.WriteBatch, index uint64) {
	d.peerStorage.applyState.AppliedIndex = index
	err := kvWb.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
	if err != nil {
		log.Errorf("[region %d] persist apply state to %d failed, err %v",
			d.regionId, index, err)
	}
}

func (d *peerMsgHandler) writeChangesToKVDB(kvWb *engine_util.WriteBatch) {
	if err := kvWb.WriteToDB(d.peerStorage.Engines.Kv); err != nil {
		log.Errorf("[region %d] write changes to kvdb failed, err %v", d.regionId, err)
	}
}

func (d *peerMsgHandler) applyCommittedEntry(entry eraftpb.Entry, kvWb *engine_util.WriteBatch) {
	if d.stopped || entry.Data == nil {
		return
	}
	var msg raft_cmdpb.RaftCmdRequest

	err := msg.Unmarshal(entry.Data)
	if err != nil {
		log.Debugf("[region %d] apply raft entry failed, err when unmarshal entry data %v", d.regionId, err)
		return
	}
	if msg.Header.RegionEpoch != nil && msg.Header.RegionEpoch.Version != d.Region().RegionEpoch.Version {
		cb := d.getCallbackFromProposals(entry.Index, entry.Term)
		cb.Done(ErrResp(&util.ErrEpochNotMatch{}))
		return
	}
	log.Debugf("[region %d] ld:%d term:%d find cb i:%d,t:%d",
		d.regionId, d.RaftGroup.Raft.Lead, d.RaftGroup.Raft.Term, entry.Index, entry.Term)
	log.Debugf("[region %d] node %d ld:%d term:%d apply raft cmd entry, i:%d,t:%d, msg:%+v",
		d.regionId, d.RaftGroup.Raft.ID(), d.RaftGroup.Raft.Lead, d.RaftGroup.Raft.Term, entry.Index, entry.Term, msg)
	// can't enclose normal requests and administrator request at same time.
	if msg.AdminRequest != nil {
		d.applyAdminCmd(kvWb, msg.AdminRequest)
	} else if len(msg.Requests) > 0 {
		d.applyMultiDataCmd(kvWb, msg.Requests, entry.Index, entry.Term)
	}
	return
}

func (d *peerMsgHandler) applyMultiDataCmd(kvWb *engine_util.WriteBatch, requests []*raft_cmdpb.Request, entryIndex, entryTerm uint64) {
	cb := d.getCallbackFromProposals(entryIndex, entryTerm)
	if cb == nil {
		if d.RaftGroup.Raft.IsLeader() {
			log.Debugf("[region %d] node %d ld:%d term:%d cannot find cb for i:%d,t:%d",
				d.regionId, d.RaftGroup.Raft.ID(), d.RaftGroup.Raft.Lead, d.RaftGroup.Raft.Term, entryIndex, entryTerm)
			return
		} else {
			// empty callback ensure follower can apply committed entries.
			cb = message.NewCallback()
		}
	}

	resps := make([]*raft_cmdpb.Response, 0, len(requests))
	for _, req := range requests {
		resp, ok := d.applyDataCmd(kvWb, entryIndex, req, cb)
		if !ok {
			log.Errorf("[region %d] node %d apply raft cmd i:%d,t:%d failed, err: %v, %+v",
				d.regionId, d.RaftGroup.Raft.ID(), entryIndex, entryTerm, cb.Resp, req)
			return
		}
		resps = append(resps, resp)
	}
	cbResp := newCmdResp()
	cbResp.Responses = resps
	log.Debugf("[region %d] node %d apply raft cmd success ld:%d term:%d, i:%d,t:%d, reqs:%+v",
		d.regionId, d.RaftGroup.Raft.ID(), d.RaftGroup.Raft.Lead, d.RaftGroup.Raft.Term, entryIndex, entryTerm, requests)
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

func (d *peerMsgHandler) applyAdminCmd(kvWb *engine_util.WriteBatch, req *raft_cmdpb.AdminRequest) {
	// todo apply admin cmd
	switch req.CmdType {
		case raft_cmdpb.AdminCmdType_CompactLog:
			d.applyCompactLog(kvWb, req.CompactLog)
	}
}

func (d *peerMsgHandler) applyCompactLog(kvWb *engine_util.WriteBatch, req *raft_cmdpb.CompactLogRequest) {
	if req.CompactIndex > d.peerStorage.applyState.TruncatedState.Index {
		d.peerStorage.applyState.TruncatedState.Index=req.CompactIndex
		d.peerStorage.applyState.TruncatedState.Term = req.CompactTerm

		d.persistApplyState(kvWb, d.peerStorage.applyState.AppliedIndex)
	}
	d.ScheduleCompactLog(req.CompactIndex)
}
