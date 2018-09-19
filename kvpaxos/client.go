package kvpaxos

import "net/rpc"
import crand "crypto/rand"
import "math/big"
import mrand "math/rand"

import "time"
import "fmt"
import "log"

type Clerk struct {
	servers []string
	// You will have to modify this struct.
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := crand.Int(crand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []string) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	return ck
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will return an
// error after a while if the server is dead.
// don't provide your own time-out mechanism.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
//
func call(srv string, rpcname string,
	args interface{}, reply interface{}) bool {
	c, errx := rpc.Dial("unix", srv)
	if errx != nil {
		fmt.Printf("%s:%s - kvpaxos Dial() failed: %v\n", srv, rpcname, errx)
		return false
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(srv, rpcname, err)
	return false
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	// log.Println("Clerk.Get", "Key:", key)
	var reply = &GetReply{}

	defer func() {
		log.Println("Clerk.Get", "Key:", key, "value:[", reply.Value, "]")
	}()
	args := GetArgs{}
	args.Key = key
	args.RandID = nrand()

	log.Println("Clerk.Get", "Key:", key, "RandID:", args.RandID)

	for {
		for i := 0; i < len(ck.servers); i++ {
			r := call(ck.servers[i], "KVPaxos.Get", args, reply)
			if !r {
				time.Sleep(time.Duration(mrand.Int()%100) * time.Millisecond)
				continue
			}
			if reply.Err != OK {
				time.Sleep(time.Duration(mrand.Int()%100) * time.Millisecond)
				continue
			}

			return reply.Value
		}
	}

	return ""
}

//
// shared by Put and Append.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{}
	args.Key = key
	args.Value = value
	args.Op = op
	args.RandID = nrand()

	log.Println("Clerk.PutAppend", "Key:", key, "value:", value, "op:", op, "RandID:", args.RandID)

	var reply = &PutAppendReply{}
	for {
		ok := false
		for i := 0; i < len(ck.servers); i++ {
			r := call(ck.servers[i], "KVPaxos.PutAppend", args, reply)
			if !r {
				time.Sleep(time.Duration(mrand.Int()%100) * time.Millisecond)
				continue
			}
			if reply.Err == OK {
				ok = true
				break
			}
			time.Sleep(time.Duration(mrand.Int()%100) * time.Millisecond)
			continue
		}
		if ok {
			break
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
