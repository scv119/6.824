package mapreduce

import (
	"fmt"
)

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	taskChannel := make(chan int, ntasks)

	for i := 0; i < ntasks; i++ {
		if phase == mapPhase {
			go func(idx int) {
				var doTaskArgs DoTaskArgs
				var reply struct{}
				doTaskArgs.JobName = mr.jobName
				doTaskArgs.File = mr.files[idx]
				doTaskArgs.Phase = phase
				doTaskArgs.TaskNumber = idx
				doTaskArgs.NumOtherPhase = nios
				for {
					worker := <-mr.registerChannel
					ok := call(worker, "Worker.DoTask", doTaskArgs, &reply)
					if ok {
						taskChannel <- idx
						mr.registerChannel <- worker
						break
					}
				}
			}(i)
		} else {
			go func(idx int) {
				var doTaskArgs DoTaskArgs
				var reply struct{}
				doTaskArgs.JobName = mr.jobName
				doTaskArgs.Phase = phase
				doTaskArgs.TaskNumber = idx
				doTaskArgs.NumOtherPhase = nios
				for {
					worker := <-mr.registerChannel
					ok := call(worker, "Worker.DoTask", doTaskArgs, &reply)
					if ok {
						taskChannel <- idx
						mr.registerChannel <- worker
						break
					}
				}
			}(i)
		}
	}

	for i := 0; i < ntasks; i++ {
		v := <-taskChannel
		fmt.Printf("Task %s number %d finsihed\n", phase, v)
	}

	fmt.Printf("Schedule: %v phase done\n", phase)
}
