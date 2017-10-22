package mapreduce

import "container/list"
import "fmt"
import "log"
import "time"

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
	var workers []*WorkerInfo
	var currMapInx = 0
	var currReduceInx = 0
	var done = false
	var failed = false
	for !done {
		select {
		case worker := <-mr.registerChannel:
			log.Printf("worker %s\n", worker)

			var workerInfo WorkerInfo
			workerInfo.address = worker

			mr.Workers[worker] = &workerInfo
			workers = append(workers, &workerInfo)
		default:
			//
			if failed {
				workers = workers[1:]
				failed = false
			}
			if currMapInx < mr.nMap {
				var mapArgs = DoJobArgs{File: mr.file, Operation: Map, JobNumber: currMapInx, NumOtherPhase: mr.nReduce}
				var mapReply DoJobReply

				ok := call(workers[0].address, "Worker.DoJob", mapArgs, &mapReply)
				if ok == false {
					log.Printf("Call map DoJob %d failed", currMapInx)
					if len(workers) < 2 {
						time.Sleep(1 * time.Second)
					}
					delete(mr.Workers, workers[0].address)
					failed = true
				} else {
					currMapInx++
				}
			} else {
				if currReduceInx < mr.nReduce {
					var reduceArgs = DoJobArgs{File: mr.file, Operation: Reduce, JobNumber: currReduceInx, NumOtherPhase: mr.nMap}
					var reduceReply DoJobReply
					ok := call(workers[0].address, "Worker.DoJob", reduceArgs, &reduceReply)
					if ok == false {
						log.Printf("Call reduce DoJob failed")
						if len(workers) < 2 {
							time.Sleep(1 * time.Second)
						}
						delete(mr.Workers, workers[0].address)
						failed = true
					} else {
						currReduceInx++
					}
				} else {
					done = true
				}
			}
		}
	}

	return mr.KillWorkers()
}
