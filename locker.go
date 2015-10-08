package lease

import (
	"errors"
	"time"
)

// A Locker provides methods to obtain and renew leases.
type Locker struct {
	store lockerStore
}

// Initialization of a Locker, requires a lockerStore.
func NewLocker(store lockerStore) *Locker {
	return &Locker{store: store}
}

// ObtainLease scans over possible leaseable items and tries to acquire a lease on any of them.
// Returns nil with an error if it can't acquire a lease.
func (l *Locker) ObtainLease(request LeaseRequest) (*Lease, error) {
	leaseIDs := l.store.ListLeaseIDs()

	for _, leaseID := range leaseIDs {
		lease, err := l.store.Lease(leaseID, request, time.Now().Add(request.LeaseDuration()))
		if err == nil { // no error means we successfully got a lease
			globalLeaseLogger.LogInfoMessage("Obtained Lease", "lessee_id", request.LesseeID(), "lease_id", lease.LeaseID)
			return lease, nil
		}
	}

	return nil, errors.New("Unable to acquire any lease")
}

// WaitUntilLeaseObtained scans over possible leaseable items and tries to acquire a lease on any of them.
// Returns as soon as it acquires a lease on one.
//
// Keeps trying indefinitely until it acquires a lease, waiting waitPeriod
// between scans of the table.
func (l *Locker) WaitUntilLeaseObtained(request LeaseRequest, waitPeriod time.Duration) *Lease {
	for {
		lease, err := l.ObtainLease(request)
		if err == nil {
			return lease
		}

		time.Sleep(waitPeriod)
	}
}

// Heartbeat starts a loop that renews the lease periodically.
// Panics if the lease is lost.
func (l *Locker) Heartbeat(lease *Lease, heartbeatDuration time.Duration) {
	var err error

	// every heartbeatDuration, try and renew the lease.
	ticker := time.NewTicker(heartbeatDuration)
	for range ticker.C {
		_, err = l.store.Lease(lease.LeaseID, lease.Request, time.Now().Add(lease.Request.LeaseDuration()))
		if err != nil {
			panic("Lease lost!")
		}
		globalLeaseLogger.LogInfoMessage("Renewed Lease", "lessee_id", lease.Request.LesseeID(), "lease_id", lease.LeaseID)
	}
}
