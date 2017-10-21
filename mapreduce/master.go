package mapreduce

import "container/list"
import "fmt"
import "log"

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

func (mr *MapReduce) RunMaster() *list.List {
	// Your code here
	var maper *WorkerInfo
	var reducer *WorkerInfo
	var workers = make([]*WorkerInfo, 0, 2)
	for i := 0; i < 2; i++ {
		worker := <-mr.registerChannel
		log.Printf("worker %s\n", worker)

		var workerInfo WorkerInfo
		workerInfo.address = worker

		mr.Workers[worker] = &workerInfo

		workers[i] = &workerInfo

		if i == 0 {
			maper = &workerInfo
		} else {
			reducer = &workerInfo
		}
	}

	var mapArgs DoJobArgs
	var mapReply DoJobReply
	for i := 0; i < mr.nMap; i++ {
		mapArgs.File = mr.file
		mapArgs.JobNumber = i
		mapArgs.NumOtherPhase = mr.nReduce
		mapArgs.Operation = Map
		ok := call(maper.address, "Worker.DoJob", mapArgs, &mapReply)
		if ok == false {
			log.Printf("Call map DoJob %d failed", i)
		}
	}

	for i := 0; i < mr.nReduce; i++ {
		var reduceArgs DoJobArgs
		var reduceReply DoJobReply
		reduceArgs.File = mr.file
		reduceArgs.JobNumber = i
		reduceArgs.NumOtherPhase = mr.nMap
		reduceArgs.Operation = Reduce
		ok := call(reducer.address, "Worker.DoJob", reduceArgs, &reduceReply)
		if ok == false {
			log.Printf("Call reduce DoJob failed")
		}
	}

	return mr.KillWorkers()
}
