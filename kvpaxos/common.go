package kvpaxos

const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	RandID int64 //随机数
	PreSeq int   //已收到回复的rpc请求的seq（paxos instance的seq）
}

type PutAppendReply struct {
	Err Err
	Seq int //当前paxos instance的seq
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	RandID int64 //随机数
	PreSeq int   //已收到回复的rpc请求的seq（paxos instance的seq）
}

type GetReply struct {
	Err   Err
	Value string
	Seq   int //当前paxos instance的seq
}
