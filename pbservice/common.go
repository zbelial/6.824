package pbservice

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongServer = "ErrWrongServer"
)

const (
	PUT     = "Put"
	APPEND  = "Append"
	FORWORD = "Forword"
	DIRECT  = "Direct"
)

type Err string

type PutAllArgs struct {
	Records map[string]string
}

type PutAllReply struct {
	Err Err
}

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
	Type    string //put; append
	ReqType string //forword; direct
	// Retry   bool
	Unique int64

	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}

// Your RPC definitions here.
