package kvpaxos

import "net"
import "fmt"
import "net/rpc"
import "log"
import "github.com/zbelial/6.824/paxos"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "time"
import "errors"

import "bytes"
import "math"

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

const (
	OpPut int = iota + 1
	OpAppend
	OpGet
	OpNil
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType int
	RandID int64 //随机数
	Key    string
	Value  string
}

type KVPaxos struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	//  Your definitions here.
	ids map[int64]bool //是否收到过id
	// logs      []*Op
	logs      map[int]Op
	minCached int            //已缓存的log的最小seq
	maxCached int            //已缓存的log的最大seq
	keySeqMap map[string]int //对于key的上次put操作的seq
}

func (kv *KVPaxos) wait(seq int, op Op) error {
	// Your code here.
	// var err error
	log.Println("KVPaxos.wait", "me:", kv.me, "Seq:", seq, "Key:", op.Key, "RandID:", op.RandID, "OpType:", op.OpType)
	// defer func() {
	// 	log.Println("KVPaxos.wait", "me:", kv.me, "Seq:", seq, "Key:", op.Key, "RandID:", op.RandID, "OpType:", op.OpType, "err:", err)
	// }()
	to := 10 * time.Millisecond
	for {
		status, v := kv.px.Status(seq)
		if status == paxos.Decided {
			r := v.(Op)
			if r.RandID == op.RandID {
				return nil
			}
			return errors.New("Conflicted")
		}
		//TODO
		if status == paxos.Forgotten {
			return errors.New("Forgotten")
		}
		time.Sleep(to)
		if to < 10*time.Second {
			to *= 2
		}
	}

	return errors.New("Error")
}

func (kv *KVPaxos) cacheLogs(seq int) {
	for i := kv.minCached; i <= kv.maxCached; i++ {
		_, ok := kv.logs[i]
		if !ok {
			s, v := kv.px.Status(i)
			if s == paxos.Decided {
				kv.logs[i] = v.(Op)
			} else {
				s, v = kv.px.Learn(i)
				if s == paxos.Decided {
					kv.logs[i] = v.(Op)
				}
			}
		}
	}

	for i := kv.maxCached + 1; i <= seq; i++ {
		s, v := kv.px.Status(i)
		if s != paxos.Decided {
			s, v = kv.px.Learn(i)
			if s == paxos.Decided {
				kv.logs[i] = v.(Op)
			}
		} else {
			kv.logs[i] = v.(Op)
		}
	}
	kv.maxCached = seq
}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	log.Println("KVPaxos.Get", "me:", kv.me, "Key:", args.Key, "RandID:", args.RandID)
	defer func() {
		log.Println("KVPaxos.Get", "me:", kv.me, "Key:", args.Key, "RandID:", args.RandID, "Err:", reply.Err, "Value:", reply.Value)
	}()

	var op Op
	op.Key = args.Key
	op.RandID = args.RandID
	op.OpType = OpGet

	seq := kv.px.Max() + 1
	for {
		kv.px.Start(seq, op)
		err := kv.wait(seq, op)
		if err == nil {
			reply.Err = OK

			log.Println("KVPaxos.Get", "me:", kv.me, "Key:", args.Key, "RandID:", args.RandID, "minCached:", kv.minCached, "maxCached:", kv.maxCached, "seq:", seq)

			kv.cacheLogs(seq)

			log.Println("KVPaxos.Get", "me:", kv.me, "Key:", args.Key, "RandID:", args.RandID, "minCached:", kv.minCached, "maxCached:", kv.maxCached, "seq2:", seq)
			dm := make(map[int64]bool)
			var buf bytes.Buffer
			for i := kv.minCached; i <= kv.maxCached; i++ {
				l, ok := kv.logs[i]
				if ok {
					if l.Key != args.Key {
						continue
					}
					_, ok := dm[l.RandID]
					if ok {
						continue
					}
					dm[l.RandID] = true

					if l.OpType == OpPut {
						log.Println("KVPaxos.Get BackTrace Put", "Key:", args.Key, "RandID:", args.RandID, "Value", l.Value, "l.RandID:", l.RandID)
						buf.Reset()
						buf.WriteString(l.Value)
					} else if l.OpType == OpAppend {
						log.Println("KVPaxos.Get BackTrace Append", "Key:", args.Key, "RandID:", args.RandID, "Value", l.Value, "l.RandID:", l.RandID)
						buf.WriteString(l.Value)
					}

				}
			}

			reply.Value = buf.String()

			break
		}

		seq++
	}
	kv.ids[args.RandID] = true

	return nil
}

func (kv *KVPaxos) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	log.Println("KVPaxos.PutAppend", "me:", kv.me, "Op:", args.Op, "Key:", args.Key, "Value:", args.Value, "RandID:", args.RandID)
	defer func() {
		log.Println("KVPaxos.PutAppend", "me:", kv.me, "Op:", args.Op, "Key:", args.Key, "Value:", args.Value, "RandID:", args.RandID, "Err:", reply.Err)
	}()

	v, ok := kv.ids[args.RandID]
	if ok {
		if v {
			reply.Err = OK
		} else {
			reply.Err = Dup
		}
		return nil
	}
	kv.ids[args.RandID] = false

	var op Op
	op.Key = args.Key
	op.Value = args.Value
	op.RandID = args.RandID
	if args.Op == "Put" {
		op.OpType = OpPut
	} else {
		op.OpType = OpAppend
	}

	seq := kv.px.Max() + 1
	for {
		kv.px.Start(seq, op)
		err := kv.wait(seq, op)
		if err == nil {
			reply.Err = OK
			break
		}

		log.Println("KVPaxos.wait retry", "me:", kv.me, "Seq:", seq, "Key:", op.Key, "RandID:", op.RandID, "OpType:", op.OpType, "err:", err)
		seq++
	}
	kv.ids[args.RandID] = true

	if op.OpType == OpPut {
		kv.keySeqMap[op.Key] = seq
		min := math.MaxInt32
		for _, s2 := range kv.keySeqMap {
			if s2 < min {
				min = s2
			}
		}

		log.Println("minCached:", kv.minCached, "maxCached:", kv.maxCached, "Min:", min)
		if min > 0 {
			kv.px.Done(min - 1)

			if min > kv.maxCached {
				min = kv.maxCached
			}

			for i := kv.minCached; i < min; i++ {
				delete(kv.logs, i)
			}
			kv.minCached = min
		}
	}

	return nil
}

// tell the server to shut itself down.
// please do not change these two functions.
func (kv *KVPaxos) kill() {
	DPrintf("Kill(%d): die\n", kv.me)
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *KVPaxos) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *KVPaxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *KVPaxos) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *KVPaxos {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(KVPaxos)
	kv.me = me

	// Your initialization code here.
	kv.ids = make(map[int64]bool)
	// kv.logs = make([]*Op, 0)
	kv.logs = make(map[int]Op)
	kv.minCached = 0
	kv.maxCached = -1
	kv.keySeqMap = make(map[string]int)

	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for kv.isdead() == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.isdead() == false {
				if kv.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && kv.isdead() == false {
				fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	return kv
}
