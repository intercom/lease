## Lease

Lease is a general DynamoDB-based lease implementation.

#### Usage

Firstly, we need to initialize a lock store that wraps DynamoDB:

```go
// get the aws Config and DynamoDB instance however you like...
config := &aws.Config{
  Region: aws.String("us-east-1")}
ddb := dynamodb.New(config)

// create a new store, specifying the table and hash key
locker := NewLockStore(ddb, "lock_table_name", "LockHashKeyName")
```

Then, we need to implement the `LeaseRequest` interface. We do this so we can get back some data from DynamoDB about the lease, and give the locker an identifier so it knows who's leasing what.

```go

type exampleLeaseRequest struct {
  processID string
}

// we only want one lease per process, so using the hostname:process_id,
func (er *exampleLeaseRequest) LesseeID() string {
  return er.processID
}

// take out a lease for 30 seconds
func (er *exampleLeaseRequest) LeaseDuration() time.Duration {
	return time.Second * 30
}

// we know that there's a SomeDataWeNeed column with some related data for the leased item
func (er *exampleLeaseRequest) AttributesToData(attributes map[string]*dynamodb.AttributeValue) (interface{}, error) {
	if attributes == nil {
		return "", nil
	}
	return *attributes["SomeDataWeNeed"].S, nil
}
```

We can use this lease request to scan over all available items to lease until we get one:

```go
// returns an error if we fail to get a lease
lease, err := locker.ObtainLease(&exampleLeaseRequest{processID: "132123"})

// or we can block until we get a lease
lease := locker.WaitUntilLeaseObtained(&exampleLeaseRequest{processID: "132123"}, time.Second * 15)
```

Finally we can tell the locker to heartbeat to keep renewing a lease:

```go
go locker.Heartbeat(lease, time.Second * 15)
```

#### TODO

- [ ] Consider whether we want gocore here (would need to make that public, it's an extra dependency...), or inject a logger.
- [ ] don't panic when things fail, not everyone wants that
- [ ] scanning operation could be nuts without ordering?
