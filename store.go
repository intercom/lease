package lease

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

var (
	GlobalLockStore *LockStore

	// LeaseNotObtainedError returned when lease not obtained
	LeaseNotObtainedError error = errors.New("Lease Not Obtained")
)

type lockerStore interface {
	Lease(workID string, request LeaseRequest, until time.Time) (*Lease, error)
	ListLeaseIDs() ([]string, error)
}

// LockStore is the lockerStore implementation for DynamoDB.
type LockStore struct {
	*dynamodb.DynamoDB
	tableName       string
	lockHashKeyName string
}

// SetupLockStoreGlobal initializes a global LockStore.
// Requires an instance of DynamoDB, a table name, and the name of the hash key for lock items.
func SetupLockStoreGlobal(ddb *dynamodb.DynamoDB, lockTableName, lockHashKeyName string) {
	GlobalLockStore = NewLockStore(ddb, lockTableName, lockHashKeyName)
}

// Returns a new LockStore.
// Requires an instance of DynamoDB, a table name, and the name of the hash key for lock items.
func NewLockStore(ddb *dynamodb.DynamoDB, lockTableName, lockHashKeyName string) *LockStore {
	return &LockStore{
		DynamoDB:        ddb,
		tableName:       lockTableName,
		lockHashKeyName: lockHashKeyName,
	}
}

// List the IDs of all lease items stored in DynamoDB.
//
// Returns a list of string PKs of Lease's;
// returns errors if failing to contact Dynamo.
func (s *LockStore) ListLeaseIDs() ([]string, error) {
	result, err := s.Scan(&dynamodb.ScanInput{
		TableName:            aws.String(s.tableName),
		ProjectionExpression: aws.String(s.lockHashKeyName),
	})
	if err != nil {
		logAWSError(err)
		return nil, err
	}

	ids := []string{}
	for _, item := range result.Items {
		ids = append(ids, *item[s.lockHashKeyName].S)
	}
	return ids, nil
}

// Attempt to acquire, or renew, a lease on the given leaseID.
//
// Returns a Lease if successfully acquired/renewed;
// if the lease is currently held by someone else, returns a LeaseNotObtainedError and nil Lease.
func (s *LockStore) Lease(leaseID string, request LeaseRequest, until time.Time) (*Lease, error) {
	item, err := s.UpdateItem(&dynamodb.UpdateItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			s.lockHashKeyName: &dynamodb.AttributeValue{
				S: aws.String(leaseID),
			},
		},
		UpdateExpression: aws.String("SET ProcessID=:process_id, LeaseUntil=:lease_until"),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":process_id": &dynamodb.AttributeValue{
				S: aws.String(request.LesseeID()),
			},
			":lease_until": &dynamodb.AttributeValue{
				N: aws.String(strconv.FormatInt(until.UnixNano(), 10)),
			},
			":time_now": &dynamodb.AttributeValue{
				N: aws.String(strconv.FormatInt(time.Now().UnixNano(), 10)),
			},
		},
		ConditionExpression: aws.String(fmt.Sprintf("(attribute_exists(%s)) and (LeaseUntil < :time_now or ProcessID = :process_id)", s.lockHashKeyName)),
		TableName:           aws.String(s.tableName),
		ReturnValues:        aws.String("ALL_NEW"),
	})
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			if awsErr.Code() == "ConditionalCheckFailedException" {
				return nil, LeaseNotObtainedError
			}
			logAWSError(err)
		} else {
			logAWSError(err)
		}
		return nil, err
	}
	leased := &Lease{LeaseID: leaseID, Request: request, Until: until, AttributeValues: item.Attributes}
	if err != nil {
		return nil, err
	}
	return leased, err
}

func logAWSError(err error) {
	if awsErr, ok := err.(awserr.Error); ok {
		if reqErr, ok := err.(awserr.RequestFailure); ok {
			globalLeaseLogger.LogErrorMessage("AWS Error",
				"Code", reqErr.Code(),
				"AWSMessage", reqErr.Message(),
				"StatusCode", reqErr.StatusCode(),
				"DDBRequestID", reqErr.RequestID())
		} else {
			globalLeaseLogger.LogErrorMessage("AWS Error",
				"Code", awsErr.Code(),
				"AWSMessage", awsErr.Message(),
				"OriginalError", awsErr.OrigErr())
		}
	} else {
		globalLeaseLogger.LogErrorMessage("AWS Error", "error", err.Error())
	}
}
