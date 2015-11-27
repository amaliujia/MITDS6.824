package mapreduce

import "container/list"
import "fmt"


type WorkerInfo struct {
	address string
	// You can add definitions here.
}


// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

func (mr *MapReduce) send_map(worker string, id int) bool{
	var jobArgs DoJobArgs
	var jobReply DoJobReply
	jobArgs.NumOtherPhase = mr.nReduce
	jobArgs.Operation = Map
	jobArgs.File = mr.file
	jobArgs.JobNumber = id
	return call(worker, "Worker.DoJob", jobArgs, &jobReply)
}

func (mr *MapReduce) send_reduce(worker string, id int) bool{
	var jobArgs DoJobArgs
	var jobReply DoJobReply
	jobArgs.NumOtherPhase = mr.nMap
	jobArgs.Operation = Reduce
	jobArgs.File = mr.file
	jobArgs.JobNumber = id
	return call(worker, "Worker.DoJob", jobArgs, &jobReply)
}

/*
 @author amaliujia
*/
func (mr *MapReduce) RunMaster() *list.List {
	// Your code here
	var mapChannel, reduceChannel = make(chan int, mr.nMap), make(chan int, mr.nReduce)

	for i := 0; i < mr.nMap; i++{
		go func(id int){
			for{
					var worker string
					var ok bool = false
					select {
					case worker = <-mr.idleChannel:
							ok = mr.send_map(worker, id)
					case worker = <-mr.registerChannel:
						  fmt.Println("Current worker: " + worker)
							ok = mr.send_map(worker, id)
					}
					if (ok) {
						// I guess this is why it is fault-tolerant, always save the
						// health workers and use these workers fist.
						// Also monitor the status of current tasks.
						mapChannel <- id
						mr.idleChannel <- worker
						return
					}
			}
		}(i)
	}

  // this is acutally barrier
	for i := 0; i < mr.nMap; i++{
		<- mapChannel
	}
	fmt.Println("Map phase is done")

	for i := 0; i < mr.nReduce; i++{
		go func(ind int){
			for {
				var worker string
				var ok = false
				select {
				case worker = <- mr.idleChannel:
					ok = mr.send_reduce(worker, ind)
				case worker = <- mr.registerChannel:
					ok = mr.send_reduce(worker, ind)
				}
				if ok{
					reduceChannel <- ind
					mr.idleChannel <- worker
					return
				}
			}
		}(i)
	}

	// this is acutally barrier.....shit...
	for i := 0; i < mr.nReduce; i++{
		<- reduceChannel
	}

	fmt.Println("Reduce is Done")

	return mr.KillWorkers()
}
