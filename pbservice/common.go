package pbservice

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongServer = "ErrWrongServer"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	Mode int32 // 0 is put, 1 is append
	RID string // enforce just once I guess
	Who string // who sends this request
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}

type ForwardArgs struct {
	DB map[string]string
}

type ForwardReply struct {
	Err Err
}
// Your RPC definitions here.
