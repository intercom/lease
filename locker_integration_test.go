// +build integration

package lease

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/stretchr/testify/assert"
)

type mongoIDsLeaseRequest struct {
	lesseeID string
}

func (mr *mongoIDsLeaseRequest) LesseeID() string {
	return mr.lesseeID
}

func (mr *mongoIDsLeaseRequest) LeaseDuration() time.Duration {
	return time.Second * 3
}

func (mr *mongoIDsLeaseRequest) AttributesToData(attributes map[string]*dynamodb.AttributeValue) (interface{}, error) {
	if attributes == nil {
		return "", nil
	}
	return *attributes["MongoAddresses"].S, nil
}

func TestIntegratedLocker(t *testing.T) {
	locker := NewLocker(setupTestStore())
	lease, err := locker.ObtainLease(&mongoIDsLeaseRequest{lesseeID: "lessee1"})
	assert.Equal(t, err, nil)
	assert.Equal(t, "1", lease.LeaseID)
	assert.Equal(t, "127.0.0.1:17017", lease.Data)
}

func TestIntegratedLockerCompetition(t *testing.T) {
	store := setupTestStore()
	locker := NewLocker(store)

	_, err := locker.ObtainLease(&mongoIDsLeaseRequest{lesseeID: "lessee1"})
	assert.Equal(t, err, nil)

	failLocker := NewLocker(store)
	_, err = failLocker.ObtainLease(&mongoIDsLeaseRequest{lesseeID: "lessee2"})
	assert.NotEqual(t, err, nil)
}

// This test relies on the argus_development_lock table being set up with a single row, with LeaseID == 1.
func setupTestStore() *LockStore {
	config := &aws.Config{
		Region: aws.String("us-east-1")}
	ddb := dynamodb.New(config)
	return NewLockStore(ddb, "argus_development_lock", "ReplicaID")
}
