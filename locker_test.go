package lease

import (
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/stretchr/testify/assert"
)

func TestLockerObtainsLease(t *testing.T) {
	store := &JustOnceInMemoryLockerStore{obtainLockAfter: 2, maxCount: 3, leaseIDs: []string{"wont-beused", "will-beused"}}
	locker := NewLocker(store)

	wi, err := locker.ObtainLease(&testLeaseRequest{})
	assert.Equal(t, nil, err)
	assert.Equal(t, "will-beused", wi.LeaseID)
}

func TestLockerHeartbeats(t *testing.T) {
	store := &JustOnceInMemoryLockerStore{obtainLockAfter: 1, maxCount: 3, leaseIDs: []string{"will-beused"}}
	locker := NewLocker(store)

	leaseRequest := &testLeaseRequest{}
	lease, err := locker.ObtainLease(leaseRequest)
	assert.Equal(t, nil, err)
	assert.Panics(t, func() {
		locker.Heartbeat(lease, 1*time.Second)
	})
	assert.Equal(t, 3, store.count)
}

type JustOnceInMemoryLockerStore struct {
	leaseIDs        []string
	obtainLockAfter int
	maxCount        int
	count           int
}

func (ls *JustOnceInMemoryLockerStore) Lease(leaseID string, request LeaseRequest, until time.Time) (*Lease, error) {
	ls.count += 1
	if ls.count >= ls.maxCount {
		return nil, errors.New("stopped")
	} else if ls.count >= ls.obtainLockAfter {
		return &Lease{LeaseID: leaseID, Request: request, AttributeValues: map[string]*dynamodb.AttributeValue{
			"MongoAddresses": &dynamodb.AttributeValue{
				S: aws.String("127.0.0.1:1234"),
			}}}, nil
	} else {
		return nil, errors.New("lock not obtained")
	}
}

func (ls *JustOnceInMemoryLockerStore) ListLeaseIDs() []string {
	return ls.leaseIDs
}

type testLeaseRequest struct {
}

func (tlr *testLeaseRequest) LesseeID() string {
	return "testrunner"
}

func (tlr *testLeaseRequest) LeaseDuration() time.Duration {
	return time.Second * 30
}
