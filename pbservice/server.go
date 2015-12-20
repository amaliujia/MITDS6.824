package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "math/rand"
import "strconv"

const Debug = 0

type PBServer struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	me         string
	vs         *viewservice.Clerk
	// Your declarations here.
	view viewservice.View
	db	map[string]string

}


func (pb *PBServer) IsPrimary() bool {
	return pb.view.Primary == pb.me
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
	pb.mu.Lock()
	// Your code here.
	if pb.db[args.Key] == "" {
		reply.Err = ErrNoKey
	} else {
		reply.Err = OK
	}
	reply.Value = pb.db[args.Key]
	pb.mu.Unlock()
	return nil
}


func (pb *PBServer) HandlePutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	if args.Mode == 0 { // put
		// fmt.Println("args put key(%v), value(%v)", args.Key, args.Value);
		pb.db[args.Key] = args.Value
		reply.Err = OK
	} else {
		j := pb.db[args.Key]
		// fmt.Println("args append key(%v), value(%v)", args.Key, j + args.Value);
		pb.db[args.Key] = j + args.Value
		reply.Err = OK
	}
}

func (pb *PBServer) CheckAtMostOnce (args *PutAppendArgs) bool {
	if pb.db["Meta-" + args.RID] != "" && pb.db["Meta-" + args.RID] == args.Who {
		return true
	}
	return false
}

func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	pb.mu.Lock()
	// Your code here.
	// fmt.Println("mode(%v)", args.Mode)

	//first check if current request have been seen and satisfied
	if pb.CheckAtMostOnce(args) == false {
		pb.HandlePutAppend(args, reply)
		// also updates backup server if current is primary server
		if pb.IsPrimary() && pb.view.Backup != "" {
			pb.ServerPut(args.Key, args.Value, pb.view.Backup, args.Mode)
		}
	}
	pb.mu.Unlock()
	return nil
}

// DBServer RPC used for inter-servers commnunication
func (pb *PBServer) ServerReceive(args *PutAppendArgs, reply *PutAppendReply) error{
	pb.mu.Lock()
	// Your code here.
	// fmt.Println("mode(%v)", args.Mode)
	pb.HandlePutAppend(args, reply)
	pb.mu.Unlock()
	return nil
}

// DBServer RPC used for inter-servers commnunication
func (pb *PBServer) ServerPut(key string, value string, rpchost string, mode int32)  {
	if Debug != 0 {
		fmt.Println("ServerPut (%v %v %v %v)", pb.view.Primary, rpchost, key, value)
	}
	// maybe fail, and backup is not complete
	args := &PutAppendArgs{key, value, mode, strconv.FormatInt(nrand(), 10), pb.view.Primary}
	var reply PutAppendReply
	flag := call(rpchost, "PBServer.ServerReceive", args, &reply)
	if flag == false {
			if Debug != 0 {
				fmt.Println("Setting Backup RPC error ", pb.view.Primary, pb.view.Backup)
			}
	}
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
	// @amaliujia call Clerk's functions ditrectly.
	v, err := pb.vs.Ping(pb.view.Viewnum)
	if err != nil {
		fmt.Println("Cannot get view from %v", pb.vs.GetServer());
	}

	// if Backup != change, send the copy of db to new backup
	if v.Backup != pb.view.Backup && pb.IsPrimary() {
		for k := range pb.db {
    	pb.ServerPut(k, pb.db[k], v.Backup, 0)
		}
	}

	pb.view = v
	pb.mu.Unlock()
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
	pb.view.Viewnum = 0
	pb.view.Primary = ""
	pb.view.Backup = ""

	pb.db = map[string]string{}


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
