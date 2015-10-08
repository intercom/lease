package lease

import (
	"time"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

// A Lease represents a lease for a given ID
type Lease struct {
	// LeaseID - HashKey value of the lease row
	LeaseID string
	// AttributeValues - Extra AttributeValues from the lease row
	AttributeValues map[string]*dynamodb.AttributeValue
	// Request - Lease Request that resulted in the Lease
	Request LeaseRequest
	// Until - Time the lease expires
	Until time.Time
}

// A LeaseRequest represents a request for a Lease.
type LeaseRequest interface {
	// LesseeID - A unique ID for the Lessee, so the lock is held for a single Lessee.
	LesseeID() string
	// LeaseDuration - How long to initially take out a lease for.
	LeaseDuration() time.Duration
}
