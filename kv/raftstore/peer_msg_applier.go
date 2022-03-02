package raftstore

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
)

func (d *peerMsgHandler) applyCmdReq(entry eraftpb.Entry) {
	if d.stopped {
		return
	}
	var msg raft_cmdpb.RaftCmdRequest

	err := msg.Unmarshal(entry.Data)
	if err != nil {
		log.Debugf("[region %d] apply raft entry failed, err when unmarshal entry data %v", d.regionId, err)
		return
	}
	// can't enclose normal requests and administrator request at same time.
	if msg.AdminRequest != nil {
		d.applyAdminCmd(msg.AdminRequest)
	} else if len(msg.Requests) > 0 {
		d.applyMultiDataCmd(msg.Requests)
	}
	return
}

func (d *peerMsgHandler) applyAdminCmd(req *raft_cmdpb.AdminRequest) {
	// todo apply admin cmd
}

func (d *peerMsgHandler) applyMultiDataCmd(requests []*raft_cmdpb.Request) {
	cb := d.getCallbackFromProposals()
	txn := d.newKvTxn(requests)
	resps := make([]*raft_cmdpb.Response, 0, len(requests))
	for _, req := range requests {
		resp, ok := d.applyDataCmd(txn, req, cb)
		if !ok {
			return
		}
		resps = append(resps, resp)
	}
	cbResp := newCmdResp()
	cbResp.Responses = resps
	cb.Done(cbResp)
	return
}

func (d *peerMsgHandler) newKvTxn(reqs []*raft_cmdpb.Request) *badger.Txn {
	needUpdate := false
	for _, req := range reqs {
		if (req.CmdType == raft_cmdpb.CmdType_Put && req.Put != nil) ||
			(req.CmdType == raft_cmdpb.CmdType_Delete && req.Delete != nil) {
			needUpdate = true
			break
		}
	}
	txn := d.ctx.engine.Kv.NewTransaction(needUpdate)
	return txn
}

func (d *peerMsgHandler) applyDataCmd(txn *badger.Txn, req *raft_cmdpb.Request, cb *message.Callback) (*raft_cmdpb.Response, bool) {
	resp := &raft_cmdpb.Response{}
	switch req.CmdType {
	case raft_cmdpb.CmdType_Get:
		{
			if err := util.CheckKeyInRegion(req.Get.Key, d.Region()); err != nil {
				cb.Done(ErrResp(err))
				return nil, false
			}
			item, err := txn.Get(engine_util.KeyWithCF(req.Get.Cf, req.Get.Key))
			if err != nil {
				cb.Done(ErrResp(err))
				return nil, false
			}
			val, err := item.ValueCopy(nil)
			if err != nil {
				cb.Done(ErrResp(err))
				return nil, false
			}
			resp.CmdType = raft_cmdpb.CmdType_Get
			resp.Get = &raft_cmdpb.GetResponse{Value: val}
		}
	case raft_cmdpb.CmdType_Snap:
		{
			cb.Txn=txn
			resp.CmdType = raft_cmdpb.CmdType_Snap
			resp.Snap= &raft_cmdpb.SnapResponse{
				Region: d.Region(),
			}
		}
	case raft_cmdpb.CmdType_Put:
		{
			if err := util.CheckKeyInRegion(req.Put.Key, d.Region()); err != nil {
				cb.Done(ErrResp(err))
				return nil, false
			}
			err := txn.Set(engine_util.KeyWithCF(req.Put.Cf, req.Put.Key), req.Put.Value)
			if err != nil {
				if err == badger.ErrTxnTooBig {
					_ = txn.Commit()
					txn = d.ctx.engine.Kv.NewTransaction(true)
					_ = txn.Set(engine_util.KeyWithCF(req.Put.Cf, req.Put.Key), req.Put.Value)
				} else {
					cb.Done(ErrResp(err))
					return nil, false
				}
			}
			resp.CmdType = raft_cmdpb.CmdType_Put
			resp.Put = &raft_cmdpb.PutResponse{}
		}
	case raft_cmdpb.CmdType_Delete:
		{
			if err := util.CheckKeyInRegion(req.Delete.Key, d.Region()); err != nil {
				cb.Done(ErrResp(err))
				return nil, false
			}
			err := txn.Delete(engine_util.KeyWithCF(req.Delete.Cf, req.Delete.Key))
			if err != nil {
				cb.Done(ErrResp(err))
				return nil, false
			}
			resp.CmdType = raft_cmdpb.CmdType_Delete
			resp.Delete = &raft_cmdpb.DeleteResponse{}
		}
	}
	return resp, true
}
