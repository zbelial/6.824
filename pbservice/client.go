package pbservice

import "github.com/zbelial/6.824/viewservice"
import "net/rpc"
import "log"
import "time"
import "crypto/rand"
import "math/big"
import "sync"

type Clerk struct {
	vs *viewservice.Clerk
	// Your declarations here
	mu   sync.Mutex
	view viewservice.View
	me   string
}

// this may come in handy.
func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(vshost string, me string) *Clerk {
	ck := new(Clerk)
	ck.vs = viewservice.MakeClerk(me, vshost)
	// Your ck.* initializations here
	ck.me = me

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
func call(srv string, rpcname string, args interface{}, reply interface{}) bool {
	c, errx := rpc.Dial("unix", srv)
	if errx != nil {
		if rpcname == "PBServer.Get" {
			var a = args.(*GetArgs)
			log.Println("Clerk call", rpcname, a.Key, a.ReqType, srv, errx)
		} else if rpcname == "PBServer.PutAppend" {
			var a = args.(*PutAppendArgs)
			log.Println("Clerk call", rpcname, a.Type, a.Key, a.Value, a.ReqType, a.Unique, srv, errx)
		} else if rpcname == "PBServer.PutAll" {
			// var a = args.(*PutAllArgs)
			log.Println("Clerk call", rpcname, srv, errx)
		} else {
			log.Println("Clerk call", rpcname, srv, errx)
		}

		return false
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	if rpcname == "PBServer.Get" {
		var a = args.(*GetArgs)
		log.Println("Clerk call", rpcname, a.Key, a.ReqType, srv, err)
	} else if rpcname == "PBServer.PutAppend" {
		var a = args.(*PutAppendArgs)
		log.Println("Clerk call", rpcname, a.Type, a.Key, a.Value, a.ReqType, a.Unique, srv, err)
	} else if rpcname == "PBServer.PutAll" {
		// var a = args.(*PutAllArgs)
		log.Println("Clerk call", rpcname, srv, err)
	} else {
		log.Println("Clerk call", rpcname, srv, err)
	}

	return false
}

//
// fetch a key's value from the current primary;
// if they key has never been set, return "".
// Get() must keep trying until it either the
// primary replies with the value or the primary
// says the key doesn't exist (has never been Put().
//
func (ck *Clerk) Get(key string) string {

	// Your code here.
	ck.mu.Lock()
	defer ck.mu.Unlock()

	v := ""
	defer func() {
		log.Printf("Clerk %s Get %s returns %s\n", ck.me, key, v)
	}()

	log.Println("Clerk", ck.me, "Get", key)
	primary := ck.primary()
	if primary == "" {
		primary = ck.cacheView()
	}

	getArgs := &GetArgs{key, FORWORD}
	getReply := &GetReply{}

	for true {
		ok := call(primary, "PBServer.Get", getArgs, getReply)
		if !ok {
			// log.Println("PBServer.Get failed")
			time.Sleep(viewservice.PingInterval)
			primary = ck.cacheView()
			continue
		}
		if getReply.Err == ErrWrongServer {
			primary = ck.cacheView()
			continue
		}

		if getReply.Err == ErrNoKey {
			v = ""
			return v
		}

		v = getReply.Value
		return v
	}

	return v
}

func (ck *Clerk) primary() string {
	return ck.view.Primary
}

func (ck *Clerk) cacheView() string {
	// log.Println("Clerk cacheView")

	for true {
		view, ok := ck.vs.Get()
		if !ok {
			continue
		}

		ck.view = view
		break
	}

	return ck.view.Primary
}

//
// send a Put or Append RPC
//
func (ck *Clerk) PutAppend(key string, value string, op string) {

	// Your code here.
	ck.mu.Lock()
	defer ck.mu.Unlock()

	putAppendArgs := &PutAppendArgs{key, value, op, DIRECT, nrand()}
	putAppendReply := &PutAppendReply{}

	primary := ck.primary()
	if primary == "" {
		primary = ck.cacheView()
	}

	log.Println("Clerk", ck.me, "PutAppend", key, value, op, primary)

	for true {
		ok := call(primary, "PBServer.PutAppend", putAppendArgs, putAppendReply)
		if !ok {
			// log.Printf("Clerk PBServer.PutAppend %s, %s failed, %s\n", key, value, primary)
			time.Sleep(viewservice.PingInterval)
			primary = ck.cacheView()

			continue
		}

		if putAppendReply.Err != OK {
			primary = ck.cacheView()
			continue
		}

		break
	}

	log.Println("Clerk", ck.me, "PutAppend", key, value, op, primary, "Finished")
}

//
// tell the primary to update key's value.
// must keep trying until it succeeds.
//
func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

//
// tell the primary to append to key's value.
// must keep trying until it succeeds.
//
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
