package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (Fate, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import "net"
import "net/rpc"
import "log"

import "os"
import "syscall"
import "sync"
import "sync/atomic"
import "fmt"
import "math"
import "math/rand"
import "reflect"
import "time"

// px.Status() return values, indicating
// whether an agreement has been decided,
// or Paxos has not yet reached agreement,
// or it was agreed but forgotten (i.e. < Min()).

// Fate ..
type Fate int

//
const (
	Decided   Fate = iota + 1
	Pending        // not yet decided.
	Forgotten      // decided but forgotten.
)

// const (
// 	PrepareOK int = iota + 1
// 	PrepareReject
// 	AcceptOK
// 	AcceptReject
// 	DecidedOk
// 	DecidedReject
// )

//
const (
	PrepareOK     = "PrepareOK"
	PrepareReject = "PrepareReject"
	AcceptOK      = "AcceptOK"
	AcceptReject  = "AcceptReject"
	DecidedOk     = "DecidedOk"
	DecidedReject = "DecidedReject"
	SeqDecided    = "SeqDecided"
)

// Acceptor ...
type Acceptor struct {
	seq    int
	pNum   int32       // highest prepare number seen
	aNum   int32       // highest accept number seen
	aValue interface{} // highest accept value seen
}

// Proposer ...
type Proposer struct {
	seq int
	num int32
}

// Instance ...
type Instance struct {
	seq    int
	status Fate //若status为Decided，则aNum/aValue为最终被接受的值？？

	pNum   int32       // acceptor highest prepare number seen
	aNum   int32       // acceptor highest accept number seen
	aValue interface{} // acceptor highest accept value seen

	num int32 // proposer num
}

// PaxosValue ...
type PaxosValue struct {
	aNum   int32
	aValue interface{}
}

// Paxos ...
type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	rpcCount   int32 // for testing
	peers      []string
	me         int // index into peers[]

	// Your data here.
	min       int               //内存中保存的instance的最小seq
	max       int               //内存中保存的instance的最大seq
	instances map[int]*Instance //(Min, Max]之间的所有instance
	peerDones []int             //保存所有peer的min
}

// PrepareArgs ...
type PrepareArgs struct {
	Seq  int
	Num  int32
	Me   int
	Done int //piggybacked done
}

// PrepareReply ...
type PrepareReply struct {
	Seq         int
	Status      string      // PrepareOK, PrepareReject, SeqDecided
	ProposeNum  int32       // PrepareArgs.Num
	AcceptNum   int32       // Instance.aNum
	AcceptValue interface{} // Instance.aValue
}

// AcceptArgs ...
type AcceptArgs struct {
	Seq   int
	Num   int32
	Me    int
	Value interface{}
}

// AcceptReply ...
type AcceptReply struct {
	Seq        int
	Status     string // AcceptOK, AcceptReject
	ProposeNum int32  // AcceptArgs.Num
}

// DecidedArgs ...
type DecidedArgs struct {
	Seq   int
	Num   int32
	Me    int
	Value interface{}
}

// DecidedReply ...
type DecidedReply struct {
	Seq    int
	Status string // DecidedOk, DecidedReject
}

// DoneArgs ...
type DoneArgs struct {
	Seq  int
	Me   int
	Done int
}

// DoneReply ...
type DoneReply struct {
}

// RPCDone ...
func (px *Paxos) RPCDone(args DoneArgs, reply *DoneReply) error {
	log.Println("RpcDone - me:", px.me, "args.Me:", args.Me, "args.Seq:", args.Seq)

	px.localDone(args, reply)

	return nil
}

func (px *Paxos) localDone(args DoneArgs, reply *DoneReply) {
	log.Println("localDone - me:", px.me, "args.Me:", args.Me, "args.Seq:", args.Seq, "args.Done:", args.Done)
	px.mu.Lock()
	defer px.mu.Unlock()

	px.peerDones[args.Me] = args.Done + 1
}

// RPCPrepare ...
func (px *Paxos) RPCPrepare(args PrepareArgs, reply *PrepareReply) error {
	log.Println("RPCPrepare - me:", px.me, "args.Me:", args.Me, "args.Seq:", args.Seq, "args.Num:", args.Num)

	px.localPrepare(args, reply)

	return nil
}

// RPCAccept ...
func (px *Paxos) RPCAccept(args AcceptArgs, reply *AcceptReply) error {
	log.Println("RPCAccept - me:", px.me, "args.Me:", args.Me, "args.Seq:", args.Seq, "args.Num:", args.Num, "args.Value:", args.Value)
	px.localAccept(args, reply)
	return nil
}

// RPCDecided ...
func (px *Paxos) RPCDecided(args DecidedArgs, reply *DecidedReply) error {
	log.Println("RPCDecided - me:", px.me, "args.Me:", args.Me, "args.Seq:", args.Seq, "args.Num:", args.Num, "args.Value:", args.Value)
	px.localDecide(args, reply)
	return nil
}

func (px *Paxos) localPrepare(args PrepareArgs, reply *PrepareReply) {
	log.Println("localPrepare - me:", px.me, "args.Me:", args.Me, "args.Seq:", args.Seq, "args.Num:", args.Num, "args.Done", args.Done)
	defer func() {
		log.Println("localPrepare - me:", px.me, "args.Me:", args.Me, "args.Seq:", args.Seq, "args.Num:", args.Num, "args.Done", args.Done, "Status:", reply.Status, "AcceptNum:", reply.AcceptNum, "AcceptValue:", reply.AcceptValue)
	}()

	px.mu.Lock()
	defer px.mu.Unlock()

	reply.Seq = args.Seq
	reply.ProposeNum = args.Num
	px.peerDones[args.Me] = args.Done
	px.freeInstance()

	instance, ok := px.instances[args.Seq]
	if !ok {
		log.Println("localPrepare - me:", px.me, "args.Me:", args.Me, "args.Seq:", args.Seq, "args.Num:", args.Num, " no instance")
		instance = &Instance{}
		instance.seq = args.Seq
		instance.status = Pending
		instance.pNum = args.Num
		instance.aNum = -1
		instance.aValue = nil

		reply.Status = PrepareOK
		reply.ProposeNum = instance.pNum
		reply.AcceptNum = instance.aNum
		reply.AcceptValue = instance.aValue

		px.instances[instance.seq] = instance

		return
	}

	log.Println("localPrepare - me:", px.me, "args.Me:", args.Me, "args.Seq:", args.Seq, "args.Num:", args.Num, "pNum", instance.pNum)
	reply.AcceptNum = instance.aNum
	reply.AcceptValue = instance.aValue
	if instance.status == Decided {
		reply.ProposeNum = instance.pNum
		reply.Status = SeqDecided

		return
	}
	if args.Num > instance.pNum {
		instance.pNum = args.Num

		reply.ProposeNum = instance.pNum
		reply.Status = PrepareOK

		px.instances[args.Seq] = instance

		return
	}
	reply.ProposeNum = instance.pNum
	reply.Status = PrepareReject
}

func (px *Paxos) getInstance(seq int) (*Instance, bool) {
	px.mu.Lock()
	defer px.mu.Unlock()
	instance, ok := px.instances[seq]

	return instance, ok
}

func (px *Paxos) setInstance(seq int, instance *Instance) {
	px.mu.Lock()
	defer px.mu.Unlock()

	px.instances[seq] = instance
}

/*
 */
func (px *Paxos) prepare(seq int, num int32) (bool, int32, int32, interface{}) {
	log.Println("====== start instance prepare - me:", px.me, "seq:", seq, "num:", num)
	prepareCount := 0
	peersCount := len(px.peers)
	for {
		accepted := make(map[int]PaxosValue)
		maxIdx := -1
		var maxNum int32
		var maxPNum int32
		for i := 0; i < peersCount; i++ {
			pa := PrepareArgs{
				Seq:  seq,
				Num:  num,
				Me:   px.me,
				Done: px.peerDones[px.me],
			}
			pl := &PrepareReply{}
			if i == px.me {
				px.localPrepare(pa, pl)
			} else {
				r := call(px.peers[i], "Paxos.RPCPrepare", pa, pl)
				if !r { //call失败，不是Prepare失败
					continue
				}
			}
			if pl.Status == SeqDecided {
				log.Println("prepare - me:", px.me, "seq:", seq, "num:", num, "SEQ HAS BEEN DECIDED")
				return true, num, pl.AcceptNum, pl.AcceptValue
			}

			if pl.ProposeNum > maxPNum {
				maxPNum = pl.ProposeNum
			}
			if pl.Status == PrepareOK {
				prepareCount++
				p := PaxosValue{
					aNum:   pl.AcceptNum,
					aValue: pl.AcceptValue,
				}
				accepted[i] = p
				if pl.AcceptNum >= maxNum {
					maxNum = pl.AcceptNum
					maxIdx = i
				}
			}
		}

		log.Println("prepare - me:", px.me, "seq:", seq, "num:", num, "prepareCount:", prepareCount, "maxIdx:", maxIdx, "maxNum:", maxNum)
		if prepareCount*2 > peersCount {
			if maxIdx != -1 {
				return false, num, accepted[maxIdx].aNum, accepted[maxIdx].aValue
			}
			return false, num, -1, nil
		}

		prepareCount = 0
		num = ((maxPNum+int32(peersCount)-1)/int32(peersCount))*int32(peersCount) + int32(px.me)

		r := rand.Int() % 100
		time.Sleep(time.Duration(r) * time.Millisecond)
	}

	return false, 0, 0, nil
}

func (px *Paxos) localAccept(args AcceptArgs, reply *AcceptReply) {
	log.Println("localAccept - me:", px.me, "args.Me:", args.Me, "args.Seq:", args.Seq, "args.Num:", args.Num, "args.Value:", args.Value)
	defer func() {
		log.Println("localAccept - me:", px.me, "args.Me:", args.Me, "args.Seq:", args.Seq, "args.Num:", args.Num, "args.Value:", args.Value, "Status:", reply.Status)
	}()

	px.mu.Lock()
	defer px.mu.Unlock()

	reply.Seq = args.Seq
	reply.ProposeNum = args.Num

	instance, ok := px.instances[args.Seq]
	if !ok {
		log.Println("localAccept - me:", px.me, "args.Me:", args.Me, "args.Seq:", args.Seq, "args.Num:", args.Num, "args.Value:", args.Value, "no instance")
		instance = &Instance{}
		instance.seq = args.Seq
		instance.status = Pending
		instance.pNum = args.Num
		instance.aNum = args.Num
		instance.aValue = args.Value

		reply.Status = AcceptOK

		px.instances[instance.seq] = instance

		return
	}

	if args.Num >= instance.pNum {
		instance.pNum = args.Num
		instance.aNum = args.Num
		instance.aValue = args.Value

		reply.Status = AcceptOK

		px.instances[args.Seq] = instance

		return
	}
	reply.Status = AcceptReject
}

func (px *Paxos) accept(seq int, num int32, value interface{}) bool {
	log.Println("====== accept - me", px.me, "seq:", seq, "num:", num, "value:", value)
	acceptCount := 0
	peersCount := len(px.peers)
	for i := 0; i < peersCount; i++ {
		pa := AcceptArgs{
			Seq:   seq,
			Num:   num,
			Me:    px.me,
			Value: value,
		}
		pl := &AcceptReply{}
		if i == px.me {
			px.localAccept(pa, pl)
		} else {
			r := call(px.peers[i], "Paxos.RPCAccept", pa, pl)
			if !r {
				continue
			}
		}
		if pl.Status == AcceptOK {
			acceptCount++
		}
	}
	if acceptCount*2 > peersCount {
		return true
	}
	return false
}

func (px *Paxos) localDecide(args DecidedArgs, reply *DecidedReply) {
	log.Println("localDecide - me", px.me, "args.Me:", args.Me, "args.Seq:", args.Seq, "args.Num:", args.Num, "args.Value:", args.Value)
	defer func() {
		log.Println("localDecide - me", px.me, "args.Me:", args.Me, "args.Seq:", args.Seq, "args.Num:", args.Num, "args.Value:", args.Value, "Status:", reply.Status)
	}()

	px.mu.Lock()
	defer px.mu.Unlock()

	instance, ok := px.instances[args.Seq]
	if !ok {
		instance = &Instance{}
		instance.seq = args.Seq
	}

	seq := args.Seq
	if seq < px.min {
		px.min = seq
	}
	if seq > px.max {
		px.max = seq
	}

	instance.pNum = args.Num
	instance.aNum = args.Num
	instance.aValue = args.Value
	instance.status = Decided
	px.instances[args.Seq] = instance

	reply.Seq = args.Seq
	reply.Status = DecidedOk
}

func (px *Paxos) decide(seq int, num int32, value interface{}) {
	//TODO 判断成功接收decide的数量？
	log.Println("====== decide - me:", px.me, "seq:", seq, "num:", num, "value:", value)
	count := 0
	peersCount := len(px.peers)
	for i := 0; i < peersCount; i++ {
		pa := DecidedArgs{
			Seq:   seq,
			Me:    px.me,
			Num:   num,
			Value: value,
		}
		pl := &DecidedReply{}
		if i == px.me {
			px.localDecide(pa, pl)
		} else {
			r := call(px.peers[i], "Paxos.RPCDecided", pa, pl)
			if !r {
				continue
			}
		}
		if pl.Status == AcceptOK {
			count++
		}
	}
}

func (px *Paxos) propose(seq int, value interface{}) {
	num := int32(px.me)
	for {
		decided, num, _, aValue := px.prepare(seq, num)
		if decided {
			px.decide(seq, num, aValue)
			break
		}

		if isNil(aValue) {
			aValue = value
		}
		ok := px.accept(seq, num, aValue)
		if !ok {
			r := rand.Int() % 1000
			time.Sleep(time.Duration(r) * time.Millisecond)
			continue
		}
		px.decide(seq, num, aValue)

		log.Println("PROPOSE FINISHED - me:", px.me, "seq:", seq, "num:", num, "value:", value)
		log.Println("")
		break
	}
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			fmt.Printf("%s:%s - paxos Dial() failed: %v\n", srv, name, err1)
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(srv, name, err)
	return false
}

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
	// Your code here.
	log.Println("Paxos.Start me:", px.me, "seq:", seq, "px.min:", px.min, "px.max:", px.max, "value:", v)
	if seq < px.min {
		return
	}

	px.mu.Lock()
	defer px.mu.Unlock()

	instance, ok := px.instances[seq]
	if !ok {
		instance := new(Instance)
		instance.seq = seq
		instance.status = Pending
		instance.num = int32(px.me)
		instance.pNum = -1
		instance.aNum = -1
		instance.aValue = nil
		px.instances[seq] = instance
	} else {
		if instance.status == Decided {
			log.Println("Paxos.Start me:", px.me, "seq:", seq, "px.min:", px.min, "px.max:", px.max, "value:", v, "SEQ HAS BEEN DECIDED")
			return
		}
	}

	go px.propose(seq, v)
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
	// Your code here.
	px.peerDones[px.me] = seq
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	// Your code here.
	return px.max
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
//
func (px *Paxos) Min() int {
	// You code here.
	return px.min
}

func (px *Paxos) freeInstance() {
	minDone := math.MaxInt32
	for i := 0; i < len(px.peerDones); i++ {
		if px.peerDones[i] < minDone {
			minDone = px.peerDones[i]
		}
	}
	if minDone+1 > px.min {
		for i := px.min; i < minDone+1; i++ {
			delete(px.instances, i)
		}

		px.min = minDone + 1
	}
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (Fate, interface{}) {
	// Your code here.
	if seq < px.min {
		return Forgotten, nil
	}

	instance, ok := px.getInstance(seq)
	if !ok {
		return Pending, nil
	}
	return instance.status, instance.aValue
}

//
// tell the peer to shut itself down.
// for testing.
// please do not change these two functions.
//
func (px *Paxos) Kill() {
	atomic.StoreInt32(&px.dead, 1)
	if px.l != nil {
		px.l.Close()
	}
}

//
// has this peer been asked to shut down?
//
func (px *Paxos) isdead() bool {
	return atomic.LoadInt32(&px.dead) != 0
}

// please do not change these two functions.
func (px *Paxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&px.unreliable, 1)
	} else {
		atomic.StoreInt32(&px.unreliable, 0)
	}
}

func (px *Paxos) isunreliable() bool {
	return atomic.LoadInt32(&px.unreliable) != 0
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.me = me

	// Your initialization code here.
	px.min = 0
	px.max = -1
	px.instances = make(map[int]*Instance)
	px.peerDones = make([]int, len(px.peers), len(px.peers))
	for i := 0; i < len(px.peers); i++ {
		px.peerDones[i] = -1
	}

	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.isdead() == false {
				conn, err := px.l.Accept()
				if err == nil && px.isdead() == false {
					if px.isunreliable() && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.isunreliable() && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					} else {
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.isdead() == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}

	return px
}

func isNil(a interface{}) bool {
	defer func() { recover() }()
	return a == nil || reflect.ValueOf(a).IsNil()
}
