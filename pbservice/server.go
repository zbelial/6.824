package pbservice

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
import "github.com/zbelial/6.824/viewservice"

type PBServer struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	me         string
	vs         *viewservice.Clerk
	// Your declarations here.
	view    viewservice.View
	records map[string]string
}

func (pb *PBServer) IsPrimary() bool {
	return pb.me == pb.view.Primary
}

func (pb *PBServer) IsBackup() bool {
	return pb.me == pb.view.Backup
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {

	pb.mu.Lock()
	defer pb.mu.Unlock()

	if !pb.IsPrimary() {
		reply.Err = ErrWrongServer
		reply.Value = ""

		return nil
	}

	// Your code here.
	v, ok := pb.records[args.Key]
	if !ok {
		reply.Err = ErrNoKey
		reply.Value = ""
	} else {
		reply.Err = OK
		reply.Value = v
	}

	return nil
}

func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {

	pb.mu.Lock()
	defer pb.mu.Unlock()

	if pb.IsPrimary() {

	} else if pb.IsBackup() {

	} else {

	}
	// Your code here.

	return nil
}

func (pb *PBServer) PutAll(args *PutAllArgs, reply *PutAllReply) error {

	// Your code here.

	return nil
}

//
// ping the viewserver periodically.
// if view changed:
//   transition to new view.
//   manage transfer of state from primary to new backup.
//
func (pb *PBServer) tick() {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	// Your code here.
	v, err := pb.vs.Ping(pb.view.Viewnum)
	if err != nil {
		//nothing
		return
	}

	if pb.IsPrimary() && v.Backup != pb.view.Backup && v.Backup != "" {
		//TODO transfer data
		args := &PutAllArgs{pb.records}
		reply := &PutAllReply{}
		if ok := call(v.Backup, "PBServer.PutAll", args, reply); !ok {
			log.Printf("PBServer tick transfer data to Backup failed")
		}
	}

	pb.view = v
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

	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

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
	}()

	go func() {
		for pb.isdead() == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
	}()

	return pb
}
