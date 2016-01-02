package kvpaxos

import "net"
import "fmt"
import "net/rpc"
import "log"
import "paxos"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "time"

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct { // Paxos value?
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key	string
	Value string
	Type string
	RID string
	Client string
}

type KVPaxos struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	// Your definitions here.
	seen			map[string]string
	db				map[string]string
	gets			map[string]string
	seed			int
}

func (kv *KVPaxos) ReachAgreement(seq int) Op {
	to := 10 * time.Millisecond
	for {
		status, v := kv.px.Status(seq)
		if status == paxos.Decided{
			return v.(Op)
		}
		time.Sleep(to)
		if to < 10 * time.Second {
			to *= 2
		}
	}
}

func (kv *KVPaxos) CommitEntries(seq int, v Op) {
	if v.Type == GET {
		prev, exist := kv.db[v.Key]
		if !exist {
			prev = ""
		}
		kv.gets[v.Client] = prev
	} else if v.Type == APPEND {
		kv.db[v.Key] = kv.db[v.Key] + v.Value
	} else {
		kv.db[v.Key] = v.Value
	}

	kv.px.Done(seq)
}

func (kv *KVPaxos) Paxos(v Op) {
	var ok bool = false
	if !ok {
		kv.seed++
		stauts, o := kv.px.Status(kv.seed)
		if stauts == paxos.Pending {
			kv.px.Start(kv.seed, v)
			agreed := kv.ReachAgreement(kv.seed)
			ok = (agreed.RID == v.RID)
			kv.CommitEntries(kv.seed, agreed)
		} else if stauts == paxos.Decided {
			kv.CommitEntries(kv.seed, o.(Op))
		}
	}
}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Err = OK
	rid := args.RID
	if kv.seen[args.Me] == rid {
		reply.Value = kv.gets[args.Me]
		return nil
	}

	kv.Paxos(Op{Key:args.Key, Type:GET, RID:args.RID, Client:args.Me})
	kv.seen[args.Me] = rid
	reply.Value = kv.gets[args.Me]
	return nil
}

func (kv *KVPaxos) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// Your code here.
	rid := args.RID
	reply.Err = OK
	if kv.seen[args.Me] == rid {
		return nil
	}

	kv.Paxos(Op{Key:args.Key, Value:args.Value, Type:args.Op, RID:args.RID, Client:args.Me})
	kv.seen[args.Me] = rid
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
	kv.db = map[string]string{}
	kv.seen = map[string]string{}
	kv.gets = map[string]string{}
	kv.seed = 0

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
