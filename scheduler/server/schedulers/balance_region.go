// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package schedulers

import (
	"sort"

	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/filter"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/operator"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/opt"
	"go.uber.org/zap"
)

func init() {
	schedule.RegisterSliceDecoderBuilder("balance-region", func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			return nil
		}
	})
	schedule.RegisterScheduler("balance-region", func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		return newBalanceRegionScheduler(opController), nil
	})
}

const (
	// balanceRegionRetryLimit is the limit to retry schedule for selected store.
	balanceRegionRetryLimit = 10
	balanceRegionName       = "balance-region-scheduler"
)

type balanceRegionScheduler struct {
	*baseScheduler
	name         string
	opController *schedule.OperatorController
	filters      []filter.Filter
}

// newBalanceRegionScheduler creates a scheduler that tends to keep regions on
// each store balanced.
func newBalanceRegionScheduler(opController *schedule.OperatorController, opts ...BalanceRegionCreateOption) schedule.Scheduler {
	base := newBaseScheduler(opController)
	s := &balanceRegionScheduler{
		baseScheduler: base,
		opController:  opController,
	}
	for _, opt := range opts {
		opt(s)
	}
	s.filters = append(s.filters, filter.StoreStateFilter{ActionScope: s.GetName(), MoveRegion: true})
	return s
}

// BalanceRegionCreateOption is used to create a scheduler with an option.
type BalanceRegionCreateOption func(s *balanceRegionScheduler)

func (s *balanceRegionScheduler) GetName() string {
	if s.name != "" {
		return s.name
	}
	return balanceRegionName
}

func (s *balanceRegionScheduler) GetType() string {
	return "balance-region"
}

func (s *balanceRegionScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.opController.OperatorCount(operator.OpRegion) < cluster.GetRegionScheduleLimit()
}

func (s *balanceRegionScheduler) Schedule(cluster opt.Cluster) *operator.Operator {
	// Your Code Here (3C).
	stores := cluster.GetStores()
	sources := filter.SelectSourceStores(stores, s.filters, cluster)
	targets := filter.SelectTargetStores(stores, s.filters, cluster)

	// the Scheduler will try the next store which has a smaller region size until all stores will have been tried.
	sort.Slice(sources, func(l, r int) bool {
		return sources[l].GetRegionSize() > sources[r].GetRegionSize()
	})
	sort.Slice(targets, func(l, r int) bool {
		return targets[l].GetRegionSize() < targets[l].GetRegionSize()
	})

	dest := targets[0]
	for i := 0; i < len(sources); i++ {
		src := sources[i]
		var region *core.RegionInfo
		for x := 0; x < balanceRegionRetryLimit; x++ {
			region = s.getSuitableRegion(src, cluster)
			if region == nil {
				log.Debug("cannot find suitable region in source store", zap.Uint64("store id", sources[i].GetID()))
				break
			}
			// we have to make sure that the difference has to be bigger than two times the approximate size of the region,
			// which ensures that after moving, the target store’s region size is still smaller than the original store.
			if src.GetRegionSize()-dest.GetRegionSize() < 2*region.GetApproximateSize() {
				log.Debugf("region size diff of source store and target store too small",
					zap.Int64("diff", src.GetRegionSize()-dest.GetRegionSize()),
					zap.Int64("region approximate size", region.GetApproximateSize()))
				continue
			}
		}

		// If the difference is big enough,
		// the Scheduler should allocate a new peer on the target
		// store and create a move peer operator.
		if op := s.allocNewPeerAndMove(src, dest, region, cluster); op != nil {
			return op
		}
		log.Debug("no operator created for selected stores", zap.String("scheduler", s.GetName()), zap.Uint64("source", src.GetID()), zap.Uint64("target", dest.GetID()))
	}

	return nil
}

func (s *balanceRegionScheduler) allocNewPeerAndMove(src *core.StoreInfo, dest *core.StoreInfo, region *core.RegionInfo, cluster opt.Cluster) *operator.Operator {
	newPeer, err := cluster.AllocPeer(dest.GetID())
	if err != nil {
		log.Errorf("alloc peer for target %d failed: %v\n", dest.GetID(), err)
		return nil
	}
	op, err := operator.CreateMovePeerOperator("balance-region", cluster, region, operator.OpBalance, src.GetID(), dest.GetID(), newPeer.GetId())
	if err != nil {
		log.Errorf("create move peer operator failed: %v\n", err)
		return nil
	}
	return op
}

// findSuitableRegion try to find the region most suitable for moving in the store for scheduler
func (s *balanceRegionScheduler) getSuitableRegion(store *core.StoreInfo, cluster opt.Cluster) (region *core.RegionInfo) {
	// First, it will try to select a pending region because pending may mean the disk is overloaded.
	cluster.GetPendingRegionsWithLock(store.GetID(), func(container core.RegionsContainer) {
		randRegion := container.RandomRegion(nil, nil)
		if randRegion != nil {
			region = randRegion
		}
	})
	if region != nil {
		return
	}
	// If there isn’t a pending region, it will try to find a follower region.
	cluster.GetFollowersWithLock(store.GetID(), func(container core.RegionsContainer) {
		randRegion := container.RandomRegion(nil, nil)
		if randRegion != nil {
			region = randRegion
		}
	})
	if region != nil {
		return
	}
	// If it still cannot pick out one region, it will try to pick leader regions.
	cluster.GetLeadersWithLock(store.GetID(), func(container core.RegionsContainer) {
		randRegion := container.RandomRegion(nil, nil)
		if randRegion != nil {
			region = randRegion
		}
	})
	return
}
