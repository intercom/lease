package lease

import (
	"errors"
	"time"
)

var (
	// LeaseLostError returned when a lease is lost
	LeaseLostError error = errors.New("Lease Lost")
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
// Returns nil with a LeaseNotObtainedError if it can't acquire a lease.
func (l *Locker) ObtainLease(request LeaseRequest) (*Lease, error) {
	leaseIDs, err := l.store.ListLeaseIDs()
	if err != nil {
		return nil, err
	}

	for _, leaseID := range leaseIDs {
		lease, err := l.store.Lease(leaseID, request, time.Now().Add(request.LeaseDuration()))
		if err == nil { // no error means we successfully got a lease
			globalLeaseLogger.LogInfoMessage("Obtained Lease", "lessee_id", request.LesseeID(), "lease_id", lease.LeaseID)
			return lease, nil
		} else if err != nil && err != LeaseNotObtainedError {
			// unexpected error
			return lease, err
		}
	}

	return nil, LeaseNotObtainedError
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
// Returns LeaseLostError if the lease is lost
// Returns other errors when failing to contact DDB.
func (l *Locker) Heartbeat(lease *Lease, heartbeatDuration time.Duration) error {
	var err error

	// every heartbeatDuration, try and renew the lease.
	ticker := time.NewTicker(heartbeatDuration)
	for range ticker.C {
		_, err = l.store.Lease(lease.LeaseID, lease.Request, time.Now().Add(lease.Request.LeaseDuration()))
		if err != nil {
			if err == LeaseNotObtainedError {
				return LeaseLostError
			}
			return err
		}
		globalLeaseLogger.LogInfoMessage("Renewed Lease", "lessee_id", lease.Request.LesseeID(), "lease_id", lease.LeaseID)
	}
	return nil
}
