// +build integration

package lease

import (
	"os"
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

func TestIntegratedLocker(t *testing.T) {
	locker := NewLocker(setupTestStore())
	lease, err := locker.ObtainLease(&mongoIDsLeaseRequest{lesseeID: "lessee1"})
	assert.Equal(t, err, nil)
	assert.Equal(t, "1", lease.LeaseID)
	assert.Equal(t, "127.0.0.1:17017", *lease.AttributeValues["MongoAddresses"].S)
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

// This test relies on a table being set up with a single row
func setupTestStore() *LockStore {
	tableName := os.Getenv("LEASETESTTABLENAME")
	hashKey := os.Getenv("LEASETESTHASHKEY")
	config := &aws.Config{
		Region: aws.String("us-east-1")}
	ddb := dynamodb.New(config)
	return NewLockStore(ddb, tableName, hashKey)
}
