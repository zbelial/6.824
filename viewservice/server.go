package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"
import "sync/atomic"
import "errors"

type ViewServer struct {
	mu       sync.Mutex
	l        net.Listener
	dead     int32 // for testing
	rpccount int32 // for testing
	me       string

	// Your declarations here.
	recentTimes  map[string]time.Time //recentTime
	currView     View                 //
	idleServers  []string             //volunteers
	serverMap    map[string]bool      //all servers
	primaryAcked bool                 //
}

func (vs *ViewServer) pickFirstIdleServer() string {
	rs := ""
	if len(vs.idleServers) > 0 {
		rs = vs.idleServers[0]
		vs.idleServers = vs.idleServers[1:]
	}

	return rs
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

	// Your code here.
	vs.mu.Lock()
	defer vs.mu.Unlock()

	log.Printf("Ping Args.Me %s, Args.Viewnum %d, Viewnum %d, Primary %s, Backup %s\n", args.Me, args.Viewnum, vs.currView.Viewnum, vs.currView.Primary, vs.currView.Backup)
	if vs.currView.Viewnum == 0 && args.Viewnum != 0 {
		return errors.New("invalid Viewnum")
	}

	_, has := vs.serverMap[args.Me]
	if !has {
		vs.serverMap[args.Me] = true
	}

	vs.recentTimes[args.Me] = time.Now()

	if vs.currView.Viewnum == 0 { //首次有server ping
		if args.Viewnum == 0 {
			vs.currView.Viewnum = 1
			vs.currView.Primary = args.Me
			vs.primaryAcked = false

			reply.View = vs.currView

			return nil
		}
	} else {
		if args.Viewnum == 0 { //新server
			log.Println("vs.primaryAcked", vs.primaryAcked)
			if !has {
				vs.idleServers = append(vs.idleServers, args.Me)
			}
			if vs.primaryAcked {
				if args.Me == vs.currView.Primary { //primary restart
					vs.currView.Viewnum++
					vs.primaryAcked = false
					vs.currView.Primary = vs.currView.Backup
					vs.currView.Backup = vs.pickFirstIdleServer()
				} else if args.Me == vs.currView.Backup { //backup restart
					vs.currView.Viewnum++
					vs.primaryAcked = false
					vs.currView.Backup = vs.pickFirstIdleServer()
					vs.idleServers = append(vs.idleServers, args.Me)
				} else {
					if vs.currView.Backup == "" {
						vs.currView.Viewnum++
						vs.primaryAcked = false
						vs.currView.Backup = vs.pickFirstIdleServer()
					}
				}
			}
			reply.View = vs.currView

			return nil
		} else {
			if args.Me == vs.currView.Primary && args.Viewnum == vs.currView.Viewnum {
				vs.primaryAcked = true

				if vs.currView.Backup == "" && len(vs.idleServers) != 0 {
					vs.currView.Backup = vs.pickFirstIdleServer()
					vs.currView.Viewnum++
					vs.primaryAcked = false

					log.Printf("new Backup %s\n", vs.currView.Backup)
				}
			}

			reply.View = vs.currView

			return nil
		}
	}

	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	vs.mu.Lock()
	defer vs.mu.Unlock()

	if vs.currView.Primary != "" && vs.currView.Backup != "" {
		log.Printf("Get Primary %s, Backup %s, Viewnum %d \n", vs.currView.Primary, vs.currView.Backup, vs.currView.Viewnum)
	}

	reply.View = vs.currView

	return nil
}

//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {

	vs.mu.Lock()
	defer vs.mu.Unlock()

	now := time.Now()
	log.Printf("ViewServer tick Primary %s, Backup %s, Viewnum %d\n", vs.currView.Primary, vs.currView.Backup, vs.currView.Viewnum)

	for s, t := range vs.recentTimes {
		if now.Sub(t) > PingInterval*DeadPings {
			log.Printf("Server %s is inactive", s)

			delete(vs.serverMap, s)
			delete(vs.recentTimes, s)
			if s == vs.currView.Primary {
				vs.currView.Primary = ""
				if vs.primaryAcked {
					vs.currView.Primary = vs.currView.Backup
					vs.currView.Backup = vs.pickFirstIdleServer()
					vs.currView.Viewnum++
					vs.primaryAcked = false
				}
			} else if s == vs.currView.Backup {
				vs.currView.Backup = ""
				if vs.primaryAcked {
					vs.currView.Backup = vs.pickFirstIdleServer()
					vs.currView.Viewnum++
					vs.primaryAcked = false
				}
			} else {
				var t []string
				for _, v := range vs.idleServers {
					if v != s {
						t = append(t, v)
					}
				}

				vs.idleServers = t
			}
		}
	}
	// Your code here.
}

//
// tell the server to shut itself down.
// for testing.
// please don't change these two functions.
//
func (vs *ViewServer) Kill() {
	atomic.StoreInt32(&vs.dead, 1)
	vs.l.Close()
}

//
// has this server been asked to shut down?
//
func (vs *ViewServer) isdead() bool {
	return atomic.LoadInt32(&vs.dead) != 0
}

// please don't change this function.
func (vs *ViewServer) GetRPCCount() int32 {
	return atomic.LoadInt32(&vs.rpccount)
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	// Your vs.* initializations here.
	vs.recentTimes = make(map[string]time.Time)
	vs.primaryAcked = false
	vs.serverMap = make(map[string]bool)

	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.isdead() == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.isdead() == false {
				atomic.AddInt32(&vs.rpccount, 1)
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.isdead() == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.isdead() == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
