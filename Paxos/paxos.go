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
import "time"
import "strconv"
import "syscall"
import "sync"
import "sync/atomic"
import "fmt"
import "math/rand"


// px.Status() return values, indicating
// whether an agreement has been decided,
// or Paxos has not yet reached agreement,
// or it was agreed but forgotten (i.e. < Min()).
type Fate int

const (
	OK = "OK"
	REJECT = "REJECT"
)

const (
	Decided   Fate = iota + 1
	Pending        // not yet decided.
	Forgotten      // decided but forgotten.
)

type Proposal struct {
		PNumber string // Proposal number
		PValue interface{} // accepted value
}

type PaxosInstance struct {
		seq int	// instance id
		clientV interface{} // chosen value
		status	Fate
		PNumber string
		accepted Proposal
}

type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	rpcCount   int32 // for testing
	peers      []string
	me         int // index into peers[]

	// your code here
	instances map[int]*PaxosInstance // instances of paxos
	dones []int
}

type PaxosArg struct {
	Seq int // instance id
	PNumber string // paxos number
	PValue interface{} // propose value
	Me int // who sends this request
	Done 			 int
}

type PaxosReply struct {
	Status	string
	PNumber string // highest PNumber seen so far
	PValue interface{} // highest PValue seen so far
}

func Assert(condition bool)  {
	if !condition {
			os.Exit(-1)
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
			fmt.Printf("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

func (px *Paxos) SelectMajority() []string {
	len := len(px.peers)
	size := int(len/2) + 1
	size = size + rand.Intn(len-size)
	targets := map[int]string{}
	acceptors := make([]string, 0)
	for i := 0; i < size; i++ {
		t := 0
		for {
			t = rand.Int() % len
			if _, exists := targets[t]; exists {
				continue
			}
			break
		}
		targets[t] = px.peers[t]
		acceptors = append(acceptors, px.peers[t])
	}
	return acceptors
}

func (px *Paxos) IsMajority(num int) bool {
	return num > len(px.peers)/2
}

func (px *Paxos) GeneratePaxosNumber() string {
	begin := time.Date(1992, time.May, 0, 0, 0, 0, 0, time.UTC)
	duration := time.Now().Sub(begin)
	return strconv.FormatInt(duration.Nanoseconds(), 10) + "-" + strconv.Itoa(px.me)
}

func (px *Paxos) MakePaxosInstance(id int, v interface{}) {
	px.instances[id] = &PaxosInstance{
		seq:id,
		clientV:v,
		status:Pending,
		PNumber:"0",
		accepted:Proposal{PNumber:"0"}}
}

func (px *Paxos) ProcessPrepare(arg *PaxosArg, reply *PaxosReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()
	index := arg.Seq
	PNum := arg.PNumber
	reply.Status = REJECT
	_, exist := px.instances[index]
	if !exist {
		px.MakePaxosInstance(index, nil)
	}

	if px.instances[index].PNumber < PNum {
		reply.Status = OK
		// at the beginning, this one is empty
		reply.PNumber = px.instances[index].accepted.PNumber
		reply.PValue = px.instances[index].accepted.PValue
		px.instances[index].PNumber = PNum
	}
	// px.mu.Unlock()
	return nil
}

func (px *Paxos) Prepare(id int, seq int, peer string, pNumber string) *PaxosReply{
	arg := &PaxosArg{Seq:seq, PNumber:pNumber, Me:px.me}
	var reply PaxosReply
	reply.Status = REJECT
	if id == px.me {
		px.ProcessPrepare(arg, &reply)
	} else {
		call(peer, "Paxos.ProcessPrepare", arg, &reply)
	}
	return &reply
}

func (px *Paxos) BroadcastPrepare(seq int, v interface{}) (bool, []string, Proposal) {
	acceptors := px.SelectMajority()
	pNumber := px.GeneratePaxosNumber()
	var num int = 0
	retPNum := "0"
	retPValue := v
	for i, acceptor := range acceptors {
		reply := px.Prepare(i, seq, acceptor, pNumber)
		if (reply == nil) {
			return false, make([]string, 0), Proposal{}
		}

		if reply.Status == OK {
			if reply.PNumber > retPNum { // get the accepted value with the largest seen value
				retPNum = reply.PNumber
				retPValue = reply.PValue
			}
			num++
		}
	}
	return px.IsMajority(num), acceptors, Proposal{PNumber:pNumber, PValue:retPValue}
}

func (px *Paxos) ProcessAccept(arg *PaxosArg, reply *PaxosReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()
	reply.Status = REJECT

	_, exist := px.instances[arg.Seq]
	if !exist {
		px.MakePaxosInstance(arg.Seq, nil)
	}
	if arg.PNumber >= px.instances[arg.Seq].PNumber {
		px.instances[arg.Seq].accepted = Proposal{arg.PNumber, arg.PValue}
		px.instances[arg.Seq].PNumber = arg.PNumber
		reply.Status = OK
	}
	// px.mu.Unlock()
	return nil
}

func (px *Paxos) BroadcastAccept(seq int, acceptors []string, p Proposal) bool {
	var num int = 0
	for i, peer := range acceptors {
		arg := &PaxosArg{Seq:seq, PNumber:p.PNumber, PValue:p.PValue, Me:px.me}
		var reply PaxosReply = PaxosReply{Status:REJECT}
		if i == px.me{
			px.ProcessAccept(arg, &reply)
		} else{
			call(peer, "Paxos.ProcessAccept", arg, &reply)
		}
		if reply.Status == OK {
			num++
		}
	}
	return px.IsMajority(num)
}

func (px *Paxos) MakeDecision(seq int, proposal Proposal) {
	_, exist := px.instances[seq]
	if !exist{
		px.MakePaxosInstance(seq,nil)
	}
	px.instances[seq].accepted = proposal
	px.instances[seq].status = Decided
}


func (px *Paxos) ProcessDecision(arg *PaxosArg, reply *PaxosReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()
	px.MakeDecision(arg.Seq, Proposal{arg.PNumber, arg.PValue})
	// how about dones?
	px.dones[arg.Me] = arg.Done
	// px.mu.Unlock()
	return nil
}

func (px *Paxos) BroadcastDecision(seq int, result Proposal) {
	arg := &PaxosArg{Seq:seq, PNumber:result.PNumber, PValue:result.PValue, Done:px.dones[px.me], Me:px.me}
	reply := PaxosReply{}

	for i := range px.peers {
		if i != px.me{
			call(px.peers[i], "Paxos.ProcessDecision", arg, &reply)
		} else {
			px.ProcessDecision(arg, &reply)
		}
	}
}

// start paxos procedure
func (px *Paxos) Paxos(seq int, v interface{}) {

	for {
		// propose phase, sleep x ms first, x from 0 to 10 ms.
		r := rand.Intn(11) - 1
		time.Sleep(time.Duration(r) * time.Millisecond)
		ok, acceptors, p := px.BroadcastPrepare(seq, v)
		if ok { // accpet phase
			ok = px.BroadcastAccept(seq, acceptors, p)
		}

		if ok { // decide phase
				px.BroadcastDecision(seq, p)
				break
		}
	}
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
	go func ()  {
			if seq < px.Min() { // should not work
				return
			}
			px.Paxos(seq, v)
	} () // return value?
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
	// Your code here.
	if px.dones[px.me] < seq {
		px.dones[px.me] = seq
	}
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	largest := 0
	for k, _ := range px.instances {
		if k > largest{
			largest = k
		}
	}
	return largest
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
	px.mu.Lock()
	defer px.mu.Unlock()
	global_min := px.dones[px.me]
	for i := range px.dones {
		if global_min > px.dones[i] {
			global_min = px.dones[i]
		}
	}
	for key, _ := range px.instances {
		if key <= global_min && px.instances[key].status == Decided {
				delete(px.instances, key)
		}
	}

	return global_min + 1
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
	min := px.Min()
	if seq < min { // instance of seq should already be deleted
		return Forgotten, nil
	}

	px.mu.Lock()
	defer px.mu.Unlock()
	if _, value := px.instances[seq];  !value{
			return Pending, nil
	}
	// px.mu.Unlock()
	return px.instances[seq].status, px.instances[seq].accepted.PValue
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
	px.instances = map[int]*PaxosInstance{}
	px.dones = make([]int, len(peers))
	for i := range peers{
		px.dones[i] = -1
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
