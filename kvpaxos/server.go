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
}

func (kv *KVPaxos) wait(seq int, op Op) error {
	// Your code here.
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

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	log.Println("KVPaxos.Get", "Key:", args.Key, "RandID:", args.RandID)
	defer func() {
		log.Println("KVPaxos.Get", "Key:", args.Key, "RandID:", args.RandID, "Err:", reply.Err, "Value:", reply.Value)
	}()
	kv.mu.Lock()
	defer kv.mu.Unlock()

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

			stack := make([]string, 0)
			for i := seq - 1; i >= 0; i-- {
				s, v := kv.px.Status(i)
				if s != paxos.Decided {
					continue
				}

				l := v.(Op)
				if l.Key != args.Key {
					continue
				}
				if l.OpType == OpPut {
					log.Println("KVPaxos.Get BackTrace Put", "Key:", args.Key, "RandID:", args.RandID, "Value", l.Value, "l.RandID:", l.RandID)
					stack = append(stack, l.Value)
					break
				} else if l.OpType == OpAppend {
					log.Println("KVPaxos.Get BackTrace Append", "Key:", args.Key, "RandID:", args.RandID, "Value", l.Value, "l.RandID:", l.RandID)
					stack = append(stack, l.Value)
				}
			}

			var buf bytes.Buffer
			for i := len(stack) - 1; i >= 0; i-- {
				buf.WriteString(stack[i])
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
	log.Println("KVPaxos.PutAppend", "Op:", args.Op, "Key:", args.Key, "Value:", args.Value, "RandID:", args.RandID)
	defer func() {
		log.Println("KVPaxos.PutAppend", "Op:", args.Op, "Key:", args.Key, "Value:", args.Value, "RandID:", args.RandID, "Err:", reply.Err)
	}()
	kv.mu.Lock()
	defer kv.mu.Unlock()

	_, ok := kv.ids[args.RandID]
	if ok {
		reply.Err = OK
		return nil
	}

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

		seq++
	}
	kv.ids[args.RandID] = true

	// if op.OpType == OpPut {
	// 	kv.px.Done(seq)
	// }

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
