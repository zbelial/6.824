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
import "errors"
import "github.com/zbelial/6.824/viewservice"

type PBServer struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	me         string
	vs         *viewservice.Clerk
	// Your declarations here.
	view       viewservice.View
	records    map[string]string
	lastUnique int64
	uniqueMap  map[int64]bool
}

func (pb *PBServer) isPrimary() bool {
	return pb.me == pb.view.Primary
}

func (pb *PBServer) isPrimary2(primary string) bool {
	return pb.me == primary
}

func (pb *PBServer) isBackup() bool {
	return pb.me == pb.view.Backup
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {

	pb.mu.Lock()
	defer pb.mu.Unlock()

	if pb.isPrimary() {
		backup := pb.view.Backup
		if backup != "" {
			bargs := &GetArgs{args.Key, FORWORD, args.Unique}
			breply := &GetReply{}
			ok := call(backup, "PBServer.Get", bargs, breply)
			if !ok {
				return errors.New("PBServer call Backup's Get failed")
			}
		}

		v, ok := pb.records[args.Key]
		if !ok {
			reply.Err = ErrNoKey
			reply.Value = ""
		} else {
			reply.Err = OK
			reply.Value = v
		}

		return nil

	} else if pb.isBackup() {
		if args.ReqType == DIRECT {
			reply.Err = ErrWrongServer
			return nil
		}

		v, ok := pb.records[args.Key]
		if !ok {
			reply.Err = ErrNoKey
			reply.Value = ""
		} else {
			reply.Err = OK
			reply.Value = v
		}

		return nil
	} else {
		reply.Err = ErrWrongServer
		reply.Value = ""

		return nil
	}

	return nil
}

func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {

	// Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()

	log.Println("PBServer PutAppend", args.Type, args.Key, args.Value, args.ReqType, args.Unique)

	if pb.isPrimary() {
		// log.Println("pb is Primary")

		pb.lastUnique = args.Unique
		backup := pb.view.Backup
		if backup != "" {
			// log.Printf("PubAppend forward to %s from %s\n", backup, pb.me)

			bargs := &PutAppendArgs{args.Key, args.Value, args.Type, FORWORD, args.Unique}
			breply := &PutAppendReply{}

			err := call(backup, "PBServer.PutAppend", bargs, breply)
			if !err {
				return errors.New("PBServer call Backup's PutAppend failed")
			}

			if breply.Err != OK {
				reply.Err = breply.Err
				return nil
			}
		}

		v, ok := pb.uniqueMap[args.Unique]
		if ok && v {
			reply.Err = OK
			return nil
		}

		if args.Type == PUT {
			pb.records[args.Key] = args.Value
		} else if args.Type == APPEND {
			v, ok := pb.records[args.Key]
			if !ok {
				pb.records[args.Key] = args.Value
			} else {
				pb.records[args.Key] = fmt.Sprintf("%s%s", v, args.Value)
			}
		}
		pb.uniqueMap[args.Unique] = true

		reply.Err = OK
		return nil

	} else if pb.isBackup() {
		// log.Println("pb is Backup")

		if args.ReqType == DIRECT {
			reply.Err = ErrWrongServer
			return nil
		}

		pb.lastUnique = args.Unique
		v, ok := pb.uniqueMap[args.Unique]
		if ok && v {
			reply.Err = OK
			return nil
		}

		if args.Type == PUT {
			pb.records[args.Key] = args.Value
		} else if args.Type == APPEND {
			v, ok := pb.records[args.Key]
			if !ok {
				pb.records[args.Key] = args.Value
			} else {
				pb.records[args.Key] = fmt.Sprintf("%s%s", v, args.Value)
			}
		}
		pb.uniqueMap[args.Unique] = true

		reply.Err = OK
		return nil

	} else {
		reply.Err = ErrWrongServer
		return nil
	}

	return nil
}

func (pb *PBServer) PutAll(args *PutAllArgs, reply *PutAllReply) error {

	// Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()

	log.Println("PBServer PutAll")

	if !pb.isBackup() {
		reply.Err = ErrWrongServer
		return nil
	}

	count := 0
	pb.records = make(map[string]string)
	for k, v := range args.Records {
		count++
		pb.records[k] = v
	}

	log.Printf("PBServer PutAll Finished, Total count %d\n", count)
	reply.Err = OK
	return nil
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

	v, err := pb.vs.Ping(pb.view.Viewnum)
	if err != nil {
		//nothing
		log.Println("PBServer Ping to Viewserver failed.", err)
		return
	}

	for {
		log.Printf("PBServer tick, Me %s, P %s, B %s, VN %d, old B %s\n", pb.me, v.Primary, v.Backup, v.Viewnum, pb.view.Backup)
		if pb.isPrimary2(v.Primary) && v.Backup != pb.view.Backup && v.Backup != "" {
			//TODO transfer data
			args := &PutAllArgs{pb.records}
			reply := &PutAllReply{}
			log.Printf("PBServer.PutAll from %s to %s\n", pb.me, v.Backup)
			ok := call(v.Backup, "PBServer.PutAll", args, reply)
			if !ok {
				log.Println("PBServer tick transfer data to Backup failed")
				v, _ = pb.vs.Ping(v.Viewnum)

				continue
			} else if reply.Err == ErrWrongServer {
				log.Println("Backup return ErrWrongServer")
				v, _ = pb.vs.Ping(v.Viewnum)

				continue
			} else {
				break
			}
		}
	}

	pb.view = v
}

// tell the server to shut itself down.
// please do not change these two functions.
func (pb *PBServer) kill() {
	atomic.StoreInt32(&pb.dead, 1)
	log.Println("PBServer kill", pb.me)
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
	log.Println("PBServer StartServer, Me", me)

	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	// Your pb.* initializations here.
	pb.records = make(map[string]string)
	pb.uniqueMap = make(map[int64]bool)

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
					log.Println("PBServer discard the request.")
					conn.Close()
				} else if pb.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					log.Println("PBServer process the request but force discard of reply.")
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						log.Printf("PBServer shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && pb.isdead() == false {
				log.Printf("PBServer(%v) accept: %v\n", me, err.Error())
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
