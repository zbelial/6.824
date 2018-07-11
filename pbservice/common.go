package pbservice

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongServer = "ErrWrongServer"
)

const (
	PUT    = "Put"
	APPEND = "Append"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
	Type   string //PUT/APPEND
	RandID int64  //随机数
	Direct bool   //primary转发or直接请求

	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Direct bool
}

type GetReply struct {
	Err   Err
	Value string
}

type TransferAllArgs struct {
	KVs map[string]string
	IDs map[int64]bool
}

type TransferAllReply struct {
	Err Err
}

// Your RPC definitions here.
