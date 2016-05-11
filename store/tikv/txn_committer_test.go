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
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/store/tikv/mock-tikv"
)

type testCommitterSuite struct {
}

var _ = Suite(&testCommitterSuite{})

func (s *testCommitterSuite) TestRegionSplit(c *C) {
	store, cluster := createMockStoreCluster(time.Millisecond * 2)
	storeID, regionIDs := mocktikv.BootstrapWithMultiRegions(cluster, []byte("g"))

	txn, err := store.Begin()
	c.Assert(err, IsNil)
	for ch := byte('a'); ch <= byte('z'); ch++ {
		err = txn.Set([]byte{ch}, []byte{ch})
		c.Assert(err, IsNil)
	}

	mocktikv.PlanSplit(cluster, regionIDs[1], []byte("m"), storeID, time.Millisecond)
	go func() {
		// Simulate concurrent txn which drops the old region.
		time.Sleep(time.Millisecond * 2)
		store.regionCache.DropRegion(regionIDs[1])
	}()
	err = txn.Commit()
	c.Assert(err, IsNil)
}
