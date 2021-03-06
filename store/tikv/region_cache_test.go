// Copyright 2016 PingCAP, Inc.
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

package tikv

import (
	"fmt"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/store/tikv/mock-tikv"
)

type testRegionCacheSuite struct {
	cluster *mocktikv.Cluster
	store1  uint64
	store2  uint64
	region1 uint64
	cache   *RegionCache
}

var _ = Suite(&testRegionCacheSuite{})

func (s *testRegionCacheSuite) SetUpTest(c *C) {
	s.cluster = mocktikv.NewCluster()
	storeIDs, regionID, _ := mocktikv.BootstrapWithMultiStores(s.cluster, 2)
	s.region1 = regionID
	s.store1 = storeIDs[0]
	s.store2 = storeIDs[1]
	s.cache = NewRegionCache(mocktikv.NewPDClient(s.cluster))
}

func (s *testRegionCacheSuite) storeAddr(id uint64) string {
	return fmt.Sprintf("store%d", id)
}

func (s *testRegionCacheSuite) TestSimple(c *C) {
	r, err := s.cache.GetRegion([]byte("a"))
	c.Assert(err, IsNil)
	c.Assert(r, NotNil)
	c.Assert(r.GetID(), Equals, s.region1)
	c.Assert(r.GetAddress(), Equals, s.storeAddr(s.store1))
	c.Assert(s.cache.regions, HasLen, 1)
}

func (s *testRegionCacheSuite) TestDropStore(c *C) {
	s.cluster.RemoveStore(s.store1)
	r, err := s.cache.GetRegion([]byte("a"))
	c.Assert(err, NotNil)
	c.Assert(r, IsNil)
	c.Assert(s.cache.regions, HasLen, 0)
}

func (s *testRegionCacheSuite) TestUpdateLeader(c *C) {
	s.cache.GetRegion([]byte("a"))
	// tikv-server reports `NotLeader`
	s.cache.UpdateLeader(s.region1, s.store2)

	r, err := s.cache.GetRegion([]byte("a"))
	c.Assert(err, IsNil)
	c.Assert(r, NotNil)
	c.Assert(r.GetID(), Equals, s.region1)
	c.Assert(r.curStoreIdx, Equals, 1)
	c.Assert(r.GetAddress(), Equals, s.storeAddr(s.store2))
}

func (s *testRegionCacheSuite) TestUpdateLeader2(c *C) {
	s.cache.GetRegion([]byte("a"))
	// new store3 becomes leader
	store3 := s.cluster.AllocID()
	s.cluster.AddStore(store3, s.storeAddr(store3))
	s.cluster.AddPeer(s.region1, store3)
	s.cluster.ChangeLeader(s.region1, store3)
	// tikv-server reports `NotLeader`
	s.cache.UpdateLeader(s.region1, store3)

	// Store3 does not exist in cache, causes a reload from PD.
	r, err := s.cache.GetRegion([]byte("a"))
	c.Assert(err, IsNil)
	c.Assert(r, NotNil)
	c.Assert(r.GetID(), Equals, s.region1)
	c.Assert(r.curStoreIdx, Equals, 0)
	c.Assert(r.GetAddress(), Equals, s.storeAddr(s.store1))

	// tikv-server reports `NotLeader` again.
	s.cache.UpdateLeader(s.region1, store3)
	r, err = s.cache.GetRegion([]byte("a"))
	c.Assert(err, IsNil)
	c.Assert(r, NotNil)
	c.Assert(r.GetID(), Equals, s.region1)
	c.Assert(r.curStoreIdx, Equals, 2)
	c.Assert(r.GetAddress(), Equals, s.storeAddr(store3))
}

func (s *testRegionCacheSuite) TestUpdateLeader3(c *C) {
	s.cache.GetRegion([]byte("a"))
	// store2 becomes leader
	s.cluster.ChangeLeader(s.region1, s.store2)
	// store2 gone, store3 becomes leader
	s.cluster.RemoveStore(s.store2)
	store3 := s.cluster.AllocID()
	s.cluster.AddStore(store3, s.storeAddr(store3))
	s.cluster.AddPeer(s.region1, store3)
	s.cluster.ChangeLeader(s.region1, store3)
	// tikv-server reports `NotLeader`(store2 is the leader)
	s.cache.UpdateLeader(s.region1, s.store2)

	// Store2 does not exist any more, causes a reload from PD.
	r, err := s.cache.GetRegion([]byte("a"))
	c.Assert(err, IsNil)
	c.Assert(r, NotNil)
	c.Assert(r.GetID(), Equals, s.region1)
	c.Assert(r.curStoreIdx, Equals, 0)
	c.Assert(r.GetAddress(), Equals, s.storeAddr(s.store1))

	// tikv-server reports `NotLeader` again.
	s.cache.UpdateLeader(s.region1, store3)
	r, err = s.cache.GetRegion([]byte("a"))
	c.Assert(err, IsNil)
	c.Assert(r, NotNil)
	c.Assert(r.GetID(), Equals, s.region1)
	c.Assert(r.curStoreIdx, Equals, 2)
	c.Assert(r.GetAddress(), Equals, s.storeAddr(store3))
}

func (s *testRegionCacheSuite) TestSplit(c *C) {
	r, err := s.cache.GetRegion([]byte("x"))
	c.Assert(err, IsNil)
	c.Assert(r.GetID(), Equals, s.region1)
	c.Assert(r.GetAddress(), Equals, s.storeAddr(s.store1))

	// split to ['' - 'm' - 'z']
	region2 := s.cluster.AllocID()
	s.cluster.Split(s.region1, region2, []byte("m"), s.store1)

	// tikv-server reports `NotInRegion`
	s.cache.DropRegion(r.GetID())
	c.Assert(s.cache.regions, HasLen, 0)

	r, err = s.cache.GetRegion([]byte("x"))
	c.Assert(err, IsNil)
	c.Assert(r.GetID(), Equals, region2)
	c.Assert(r.GetAddress(), Equals, s.storeAddr(s.store1))
	c.Assert(s.cache.regions, HasLen, 1)
}

func (s *testRegionCacheSuite) TestMerge(c *C) {
	// ['' - 'm' - 'z']
	region2 := s.cluster.AllocID()
	s.cluster.Split(s.region1, region2, []byte("m"), s.store2)

	r, err := s.cache.GetRegion([]byte("x"))
	c.Assert(err, IsNil)
	c.Assert(r.GetID(), Equals, region2)

	// merge to single region
	s.cluster.Merge(s.region1, region2)

	// tikv-server reports `NotInRegion`
	s.cache.DropRegion(r.GetID())
	c.Assert(s.cache.regions, HasLen, 0)

	r, err = s.cache.GetRegion([]byte("x"))
	c.Assert(err, IsNil)
	c.Assert(r.GetID(), Equals, s.region1)
	c.Assert(s.cache.regions, HasLen, 1)
}

func (s *testRegionCacheSuite) TestReconnect(c *C) {
	s.cache.GetRegion([]byte("a"))

	// connect tikv-server failed, cause drop cache
	s.cache.DropRegion(s.region1)

	r, err := s.cache.GetRegion([]byte("a"))
	c.Assert(err, IsNil)
	c.Assert(r, NotNil)
	c.Assert(r.GetID(), Equals, s.region1)
	c.Assert(r.GetAddress(), Equals, s.storeAddr(s.store1))
	c.Assert(s.cache.regions, HasLen, 1)
}

func (s *testRegionCacheSuite) TestNextStore(c *C) {
	region, err := s.cache.GetRegion([]byte("a"))
	c.Assert(err, IsNil)
	c.Assert(region.curStoreIdx, Equals, 0)

	s.cache.NextStore(s.region1)
	region, err = s.cache.GetRegion([]byte("a"))
	c.Assert(err, IsNil)
	c.Assert(region.curStoreIdx, Equals, 1)

	s.cache.NextStore(s.region1)
	region, err = s.cache.GetRegion([]byte("a"))
	c.Assert(err, IsNil)
	// Out of range of Stores, so get Region again and pick Stores[0] as leader.
	c.Assert(region.curStoreIdx, Equals, 0)
}
