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
	"math/rand"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/kvproto/pkg/errorpb"
	pb "github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/pd/pd-client"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/store/tikv/oracle/oracles"
)

type storeCache struct {
	mu    sync.Mutex
	cache map[string]*tikvStore
}

var mc storeCache

// Driver implements engine Driver.
type Driver struct {
}

// Open opens or creates an TiKV storage with given path.
// Path example: tikv://etcd-node1:port,etcd-node2:port/pd-path?cluster=1
func (d Driver) Open(path string) (kv.Storage, error) {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	etcdAddrs, pdPath, clusterID, err := parsePath(path)
	if err != nil {
		return nil, errors.Trace(err)
	}
	// FIXME: uuid will be a very long and ugly string, simplify it.
	uuid := fmt.Sprintf("tikv-%v-%v-%v", etcdAddrs, pdPath, clusterID)
	if store, ok := mc.cache[uuid]; ok {
		return store, nil
	}

	pdCli, err := pd.NewClient(etcdAddrs, pdPath, clusterID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	s := newTikvStore(uuid, pdCli, NewRPCClient)
	mc.cache[uuid] = s
	return s, nil
}

type tikvStore struct {
	mu     sync.Mutex
	uuid   string
	oracle oracle.Oracle
	// addr => *Client
	clients       map[string]Client
	clientFactory ClientFactory
	clientLock    sync.RWMutex
	regionCache   *RegionCache
}

func newTikvStore(uuid string, pdClient pd.Client, factory ClientFactory) *tikvStore {
	return &tikvStore{
		uuid:          uuid,
		oracle:        oracles.NewPdOracle(pdClient),
		clients:       make(map[string]Client),
		clientFactory: factory,
		regionCache:   NewRegionCache(pdClient),
	}
}

func (s *tikvStore) getClient(addr string) (Client, error) {
	s.clientLock.RLock()
	client, ok := s.clients[addr]
	s.clientLock.RUnlock()
	if ok {
		return client, nil
	}

	s.clientLock.Lock()
	defer s.clientLock.Unlock()

	client, err := s.clientFactory(addr)
	if err != nil {
		return nil, errors.Trace(err)
	}

	s.clients[addr] = client
	return client, nil
}

// getRegion returns Region by key.
func (s *tikvStore) getRegion(k []byte) (*requestRegion, error) {
	region, err := s.regionCache.GetRegion(k)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &requestRegion{
		Region:    region,
		lookupKey: k,
	}, nil
}

// locateKey returns Region's versioned ID by key.
func (s *tikvStore) locateKey(k []byte) (VersionedRegionID, error) {
	region, err := s.regionCache.GetRegion(k)
	if err != nil {
		var vid VersionedRegionID
		return vid, errors.Trace(err)
	}
	return region.GetVersionedID(), nil
}

func (s *tikvStore) Begin() (kv.Transaction, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	txn, err := newTiKVTxn(s)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return txn, nil
}

func (s *tikvStore) GetSnapshot(ver kv.Version) (kv.Snapshot, error) {
	snapshot := newTiKVSnapshot(s, ver)
	return snapshot, nil
}

func (s *tikvStore) Close() error {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	delete(mc.cache, s.uuid)

	var lastErr error
	for addr, client := range s.clients {
		err := client.Close()
		if err != nil {
			log.Errorf("Close client error[%s]", err)
			lastErr = err
		}
		delete(s.clients, addr)
	}
	return lastErr
}

func (s *tikvStore) UUID() string {
	return s.uuid
}

func (s *tikvStore) CurrentVersion() (kv.Version, error) {
	startTS, err := s.oracle.GetTimestamp()
	if err != nil {
		return kv.NewVersion(0), errors.Trace(err)
	}

	return kv.NewVersion(startTS), nil
}

// sendKVReq sends req to tikv server. It will retry internally to find the right
// region leader if i) fails to establish a connection to server or ii) server
// returns `NotLeader`.
func (s *tikvStore) SendKVReq(req *pb.Request, region *requestRegion) (*pb.Response, error) {
	var backoffErr error
	for backoff := rpcBackoff(); backoffErr == nil; backoffErr = backoff() {
		var err error
		region, err = s.getRegion(region.GetLookupKey())
		if err != nil {
			return nil, errors.Trace(err)
		}
		client, err := s.getClient(region.GetAddress())
		if err != nil {
			return nil, errors.Trace(err)
		}
		req.Context = region.GetContext()
		resp, err := client.SendKVReq(req)
		if err != nil {
			log.Warnf("send tikv request error: %v, try next store later", err)
			s.regionCache.NextStore(region.GetID())
			continue
		}
		if regionErr := resp.GetRegionError(); regionErr != nil {
			// Retry if error is `NotLeader`.
			if notLeader := regionErr.GetNotLeader(); notLeader != nil {
				log.Warnf("tikv reports `NotLeader`: %s, retry later", notLeader.String())
				s.regionCache.UpdateLeader(notLeader.GetRegionId(), notLeader.GetLeaderStoreId())
				continue
			}
			// For other errors, we only drop cache here.
			// Because caller may need to re-split the request.
			log.Warnf("tikv reports region error: %v", resp.GetRegionError())
			s.regionCache.DropRegion(req.GetContext().GetRegionId())
			return resp, nil
		}
		if resp.GetType() != req.GetType() {
			return nil, errors.Trace(errMismatch(resp, req))
		}
		return resp, nil
	}
	return nil, errors.Trace(backoffErr)
}

// SendKVReqByVersionedID sends req to tikv server. It will retry internally to
// find the right region leader if i) fails to establish a connection to server
// or ii) server returns `NotLeader`.
func (s *tikvStore) SendKVReqByVersionedID(req *pb.Request, vid VersionedRegionID) (*pb.Response, error) {
	var backoffErr error
	for backoff := rpcBackoff(); backoffErr == nil; backoffErr = backoff() {
		region := s.regionCache.GetRegionByVersionedID(vid)
		if region == nil {
			return &pb.Response{
				RegionError: &errorpb.Error{
					StaleEpoch: &errorpb.StaleEpoch{},
				},
			}, nil
		}
		client, err := s.getClient(region.GetAddress())
		if err != nil {
			return nil, errors.Trace(err)
		}
		req.Context = region.GetContext()
		resp, err := client.SendKVReq(req)
		if err != nil {
			log.Warnf("send tikv request error: %v, try next store later", err)
			s.regionCache.NextStore(region.GetID())
			continue
		}
		if regionErr := resp.GetRegionError(); regionErr != nil {
			// Retry if error is `NotLeader`.
			if notLeader := regionErr.GetNotLeader(); notLeader != nil {
				log.Warnf("tikv reports `NotLeader`: %s, retry later", notLeader.String())
				s.regionCache.UpdateLeader(notLeader.GetRegionId(), notLeader.GetLeaderStoreId())
				continue
			}
			// For other errors, we only drop cache here.
			// Because caller may need to re-split the request.
			log.Warnf("tikv reports region error: %v", resp.GetRegionError())
			s.regionCache.DropRegion(req.GetContext().GetRegionId())
			return resp, nil
		}
		if resp.GetType() != req.GetType() {
			return nil, errors.Trace(errMismatch(resp, req))
		}
		return resp, nil
	}
	return nil, errors.Trace(backoffErr)
}

func parsePath(path string) (etcdAddrs []string, pdPath string, clusterID uint64, err error) {
	var u *url.URL
	u, err = url.Parse(path)
	if err != nil {
		err = errors.Trace(err)
		return
	}
	if strings.ToLower(u.Scheme) != "tikv" {
		log.Errorf("Uri scheme expected[tikv] but found [%s]", u.Scheme)
		err = errors.Trace(err)
		return
	}
	clusterID, err = strconv.ParseUint(u.Query().Get("cluster"), 10, 64)
	if err != nil {
		log.Errorf("Parse clusterID error [%s]", err)
		err = errors.Trace(err)
		return
	}
	etcdAddrs = strings.Split(u.Host, ",")
	pdPath = u.Path
	return
}

type requestRegion struct {
	*Region
	// lookupKey is used for getting region meta.
	lookupKey []byte
}

// GetLookupKey return lookupKey.
func (r *requestRegion) GetLookupKey() []byte {
	return r.lookupKey
}

func init() {
	mc.cache = make(map[string]*tikvStore)
	rand.Seed(time.Now().UnixNano())
}
