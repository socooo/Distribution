package mapreduce

import (
	"fmt"
	"sync"
	"log"
	"time"
)

//
// schedule() starts and waits for all tasks in the given phase (Map
// or Reduce). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//
	workersGroup := sync.WaitGroup{}
	workersGroup.Add(ntasks)

	for taskp:=0; taskp < ntasks; taskp++{
		go func(taskNo int, phase jobPhase, wg *sync.WaitGroup) {
			var callOk bool
			for{
				worker, tchanOk := <- registerChan
				if !tchanOk {
					log.Fatal("Error when read registerChan")
				}
				switch phase {
				case mapPhase:
					doTaskArg := DoTaskArgs{
						jobName,
						mapFiles[taskNo],
						phase,
						taskNo,
						n_other}
					callOk = call(worker, "Worker.DoTask", doTaskArg, new(struct{}))
				case reducePhase:
					doTaskArg := DoTaskArgs{
						jobName,
						"",
						phase,
						taskNo,
						n_other}
					callOk = call(worker, "Worker.DoTask", doTaskArg, new(struct{}))
				}
				if !callOk{
					time.Sleep(time.Second)
				}
				if callOk {
					go func(worker string){		// 启动新线程 将 worker 加入到 registerChan 中，否则最后一个执行task的线程会被阻塞。
						registerChan <- worker
					}(worker)
					break
				} else {
					go func(worker string){		// 启动新线程 将 worker 加入到 registerChan 中，否则最后一个执行task的线程会被阻塞。
						registerChan <- worker
					}(worker)
				}
			}
			defer wg.Done()
		}(taskp, phase, &workersGroup)
	}
	workersGroup.Wait()
	fmt.Printf("Schedule: %v phase done\n", phase)
}
