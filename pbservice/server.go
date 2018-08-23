package pbservice

import "github.com/zbelial/6.824/viewservice"

// import "viewservice"
import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "math/rand"

type PBServer struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	me         string
	vs         *viewservice.Clerk
	// Your declarations here.
	view   viewservice.View
	kvs    map[string]string
	ids    map[int64]bool //是否收到过id
	synced bool           // 对于新的backup，是否已经从primary同步了所有数据
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()

	log.Println("PBServer - Get", pb.me, args.Key, args.Direct)

	defer func() {
		// log.Println("PBServer - Get", pb.me, args.Key, args.Direct, reply.Err, reply.Value, "Finished")
		log.Println("PBServer - Get", pb.me, args.Key, args.Direct, reply.Err, "Finished")
	}()

	reply.Err = OK
	reply.Value = ""
	if pb.isPrimary() {
		if !args.Direct {
			reply.Err = ErrWrongServer
		} else {
			v, ok := pb.kvs[args.Key]
			if !ok {
				reply.Value = ""
			}
			reply.Value = v

			if pb.view.Backup != "" {
				bArgs := &GetArgs{Key: args.Key, Direct: false}
				bReply := &GetReply{}

				ok := call(pb.view.Backup, "PBServer.Get", bArgs, bReply)
				if !ok {
					reply.Err = ErrWrongServer
					return nil
				}

				if bReply.Err == ErrWrongServer {
					reply.Err = ErrWrongServer
					return nil
				}

				if reply.Value != bReply.Value {
					reply.Err = ErrWrongServer
					return nil
				}
			}
		}
	} else if pb.isBackup() {
		if !pb.synced {
			reply.Err = ErrWrongServer
		}

		if args.Direct {
			reply.Err = ErrWrongServer
		} else {
			v, ok := pb.kvs[args.Key]
			if !ok {
				reply.Value = ""
			}
			reply.Value = v
		}
	} else {
		reply.Err = ErrWrongServer
	}

	return nil
}

func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {

	// Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()

	log.Println("PBServer - PutAppend", pb.me, args.Key, args.Value, args.Direct, args.RandID)
	defer func() {
		log.Println("PBServer - PutAppend", pb.me, args.Key, args.Value, args.Direct, args.RandID, reply.Err, "Finished")
	}()

	reply.Err = OK
	if pb.isPrimary() {
		if !args.Direct {
			reply.Err = ErrWrongServer
		} else {
			_, ok := pb.ids[args.RandID]
			if !ok {
				pb.ids[args.RandID] = true
				pb.putAppend(args.Key, args.Value, args.Type)
			}

			if pb.view.Backup != "" {
				bArgs := &PutAppendArgs{}
				bReply := &PutAppendReply{}
				bArgs.Key = args.Key
				bArgs.Value = args.Value
				bArgs.Type = args.Type
				bArgs.Direct = false
				bArgs.RandID = args.RandID

				ok := call(pb.view.Backup, "PBServer.PutAppend", bArgs, bReply)
				if !ok {
					reply.Err = ErrWrongServer
					return nil
				}
				if bReply.Err == ErrWrongServer {
					reply.Err = ErrWrongServer
					return nil
				}
			}

		}
	} else if pb.isBackup() {
		if !pb.synced {
			reply.Err = ErrWrongServer
		}
		if args.Direct {
			reply.Err = ErrWrongServer
		} else {
			_, ok := pb.ids[args.RandID]
			if !ok {
				pb.ids[args.RandID] = true
				pb.putAppend(args.Key, args.Value, args.Type)
			}
		}
	} else {
		reply.Err = ErrWrongServer
	}

	return nil
}

func (pb *PBServer) putAppend(key, value, opType string) {
	v, ok := pb.kvs[key]
	if !ok {
		v = ""
	}
	if opType == APPEND {
		pb.kvs[key] = fmt.Sprintf("%s%s", v, value)
	} else {
		pb.kvs[key] = value
	}
}

func (pb *PBServer) TransferAll(args *TransferAllArgs, reply *TransferAllReply) error {

	// Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()

	log.Println("PBServer - TransferAll")
	defer func() {
		log.Println("PBServer - TransferAll", reply.Err)
	}()

	reply.Err = OK
	if !pb.isBackup() {
		reply.Err = ErrWrongServer
		return nil
	}

	pb.kvs = args.KVs
	pb.ids = args.IDs
	pb.synced = true

	return nil
}

func (pb *PBServer) TransferToBackup() {
	log.Println("PBServer - TransferToBackup", pb.view.Primary, pb.view.Backup)

	pb.mu.Lock()
	kvs := make(map[string]string)
	for k, v := range pb.kvs {
		kvs[k] = v
	}
	ids := make(map[int64]bool)
	for k, v := range pb.ids {
		ids[k] = v
	}
	args := &TransferAllArgs{KVs: kvs, IDs: ids}
	reply := &TransferAllReply{}
	pb.mu.Unlock()

	for {
		if !pb.isPrimary() {
			break
		}

		ok := call(pb.view.Backup, "PBServer.TransferAll", args, reply)
		if !ok {
			continue
		}
		if reply.Err != OK {
			log.Println("PBServer - TransferAll", reply.Err)
			continue
		}
		break
	}

	log.Println("PBServer - TransferToBackup", pb.view.Primary, pb.view.Backup, reply.Err, "Finished")
}

//
// ping the viewserver periodically.
// if view changed:
//   transition to new view.
//   manage transfer of state from primary to new backup.
//
func (pb *PBServer) tick() {

	// Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()

	view, err := pb.vs.Ping(pb.view.Viewnum)
	if err != nil {
		fmt.Println(err)
		return
	}

	oldView := pb.view
	if view.Viewnum != oldView.Viewnum {
		pb.view = view
		if pb.isPrimary() && oldView.Backup != view.Backup && view.Backup != "" {
			go pb.TransferToBackup()
		}
	}
}

func (pb *PBServer) isPrimary() bool {
	return pb.view.Primary == pb.me
}

func (pb *PBServer) isBackup() bool {
	return pb.view.Backup == pb.me
}

// tell the server to shut itself down.
// please do not change these two functions.
func (pb *PBServer) kill() {
	atomic.StoreInt32(&pb.dead, 1)
	pb.l.Close()
}

// call this to find out if the server is dead.
func (pb *PBServer) isdead() bool {
	return atomic.LoadInt32(&pb.dead) != 0
}

// please do not change these two functions.
func (pb *PBServer) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&pb.unreliable, 1)
	} else {
		atomic.StoreInt32(&pb.unreliable, 0)
	}
}

func (pb *PBServer) isunreliable() bool {
	return atomic.LoadInt32(&pb.unreliable) != 0
}

func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	// Your pb.* initializations here.
	pb.view = viewservice.View{}
	pb.kvs = make(map[string]string)
	pb.ids = make(map[int64]bool)
	pb.synced = false

	log.Println("StartServer - 1")
	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	log.Println("StartServer - 2")
	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l
	log.Println("StartServer - 3")

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for pb.isdead() == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.isdead() == false {
				if pb.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.isunreliable() && (rand.Int63()%1000) < 200 {
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
			if err != nil && pb.isdead() == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}

		log.Println(pb.me, "is killed 1")
	}()

	go func() {
		for pb.isdead() == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
		log.Println(pb.me, "is killed 2")
	}()

	return pb
}
