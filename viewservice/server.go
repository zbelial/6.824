package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"
import "sync/atomic"

type ViewServer struct {
	mu       sync.Mutex
	l        net.Listener
	dead     int32 // for testing
	rpccount int32 // for testing
	me       string

	//           Your declarations here.
	view         View
	pingTimes    map[string]time.Time //记录string对应的server最近ping的时间
	recentViews  map[string]uint      //记录string对应的server所知的最新viewnum
	volunteers   map[string]int32     //记录string对应的server作为空闲server
	acknowledged bool
	startup      bool
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {
	// Your code here.
	vs.mu.Lock()
	defer vs.mu.Unlock()

	now := time.Now()
	vs.pingTimes[args.Me] = now
	vs.recentViews[args.Me] = args.Viewnum

	if vs.startup {
		vs.startup = false
		vs.view.Primary = args.Me
		vs.view.Viewnum = 1
		vs.acknowledged = false

		reply.View = vs.view

		return nil
	}

	if args.Me == vs.view.Primary {
		if args.Viewnum == vs.view.Viewnum {
			vs.acknowledged = true
		}
	} else if args.Me == vs.view.Backup {

	} else {
		vs.volunteers[args.Me] = 1
	}
	reply.View = vs.view

	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	vs.mu.Lock()
	defer vs.mu.Unlock()

	reply.View = vs.view

	return nil
}

//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {

	// Your code here.
	vs.mu.Lock()
	defer vs.mu.Unlock()

	now := time.Now()

	if vs.view.Primary != "" {
		vt := vs.pingTimes[vs.view.Primary]
		if now.Sub(vt) > DeadPings*PingInterval { //primary died
			delete(vs.pingTimes, vs.view.Primary)

			if vs.acknowledged {
				if vs.view.Backup != "" {
					vs.advance(true)
				}
			}
		} else {
			pv := vs.recentViews[vs.view.Primary]
			if pv == 0 { // primary restarted
				if vs.acknowledged {
					if vs.view.Backup != "" {
						vs.volunteers[vs.view.Primary] = 1
						vs.advance(true)
					}
				}
			}
		}
	}

	if vs.view.Backup != "" {
		vt := vs.pingTimes[vs.view.Backup]
		if now.Sub(vt) > DeadPings*PingInterval { //backup died
			delete(vs.pingTimes, vs.view.Backup)

			if vs.acknowledged {
				vs.advance(false)
			}
		} else {
			pv := vs.recentViews[vs.view.Backup]
			if pv == 0 { //back restarted
				if vs.acknowledged {
					vs.volunteers[vs.view.Backup] = 1
					vs.advance(false)
				}
			}
		}
	} else {
		if vs.acknowledged && len(vs.volunteers) > 0 { //no backup and there is an idle server
			vs.advance(false)
		}
	}

	for k, t := range vs.pingTimes {
		if now.Sub(t) >= DeadPings*PingInterval {
			delete(vs.pingTimes, k)
			delete(vs.volunteers, k)
			delete(vs.recentViews, k)
		}
	}
}

func (vs *ViewServer) advance(primary bool) {
	log.Println("ViewServer - advance", primary, vs.view.Primary, vs.view.Backup, vs.view.Viewnum)
	defer func() {
		log.Println("ViewServer - advance", primary, vs.view.Primary, vs.view.Backup, vs.view.Viewnum, "Finished")
	}()

	if primary {
		vs.view.Primary = vs.view.Backup
	}
	vs.view.Backup = vs.getVolunteer()
	vs.acknowledged = false
	vs.view.Viewnum += 1
}

func (vs *ViewServer) getVolunteer() string {
	var v string
	if len(vs.volunteers) > 0 {
		for k, _ := range vs.volunteers {
			v = k
			break
		}
		delete(vs.volunteers, v)
	}

	return v
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
	vs.pingTimes = make(map[string]time.Time)
	vs.volunteers = make(map[string]int32)
	vs.recentViews = make(map[string]uint)
	vs.acknowledged = false
	vs.startup = true

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
