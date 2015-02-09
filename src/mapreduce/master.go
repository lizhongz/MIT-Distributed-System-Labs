package mapreduce
import "container/list"
import "fmt"
import "log"

const (
  Idle = "idle"
  InPorcess = "in-process"
  Completed = "completed"
)

type WorkerState string

type WorkerInfo struct {
  address string

  // You can add definitions here.
  state WorkerState
  jobs []int // completed jobs
}

const (
  Wait = "wait"
  Assigned = "assigned"
  Done = "done"
)

type JobState string

type JobInfo struct {
    id int
    worker string // if worker != nil, this job is assigned
    state JobState
}

func (mr *MapReduce) GetIdleWorker() string {
  for addr, wk := range mr.Workers {
    if wk.state == Idle {
      return addr
    }
  }
  return ""
}

// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
  l := list.New()
  for _, w := range mr.Workers {
    DPrintf("DoWork: shutdown %s\n", w.address)
    args := &ShutdownArgs{}
    var reply ShutdownReply;
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
  DPrintf("RunMaster %s\n", mr.MasterAddress)
  mr.StartRegistrationServer()

  // Retrieve worker registeration messages from RegisterChannel
  go func() {
    for {
      addr := <-mr.registerChannel
      mr.Workers[addr] = new(WorkerInfo)
      mr.Workers[addr].address = addr
      mr.Workers[addr].state = Idle
      mr.Workers[addr].jobs = make([]int, 0, 8)
      DPrintf("RunMaster: worker %s is registered\n", addr)
    }
  }()

  // Initialize Map jobs' info
  for i := 0; i < mr.nMap; i++ {
    mr.MapJobs[i] = new(JobInfo)
    mr.MapJobs[i].id = i
    mr.MapJobs[i].state = Wait
  }

  // Monitor Map job completion on the MapJobChan channel
  mapDone := make(chan bool)
  go func() {
    for cnt := 0; cnt < mr.nMap; cnt++ {
      doneJob := <-mr.MapJobChan
      mr.MapJobs[doneJob].state = "done"
      addr := mr.MapJobs[doneJob].worker
      mr.Workers[addr].jobs = append(mr.Workers[addr].jobs, doneJob)
      mr.Workers[addr].state = "idle"
      DPrintf("RunMaster: Map Job %d is done\n", doneJob)
    }
    DPrintf("RunMaster: all Map jobs are done\n")
    mapDone <- true
  }()

  // Assign nMap jobs to workers
  for jobId := 0; jobId < mr.nMap; {
    addr := mr.GetIdleWorker()
    if addr == "" {
        continue;
    }
    DPrintf("RunMaster: Assign Map job %d to worker %s\n", jobId, addr)
    arg := &DoJobArgs{mr.file, Map, jobId, nReduce}
    res := &DoJobReply{}
    mr.MapJobs[jobId].worker = addr
    mr.MapJobs[jobId].state = Assigned
    go func() {
      succ := call(addr, "Worker.DoJob", arg, res)
      if succ {
        mr.MapJobChan <- arg.JobNumber
      } else {
        log.Fatalf("Master: failed to execute Map Job %d on woker %s",
          arg.JobNumber, addr)
      }
    }()
    jobId++
  }

  // Wait all map jobs are finished
  <-mapDone

  // Initialize Reduce jobs' info
  for i := 0; i < mr.nReduce; i++ {
    mr.ReduceJobs[i] = new(JobInfo)
    mr.ReduceJobs[i].id = i
    mr.ReduceJobs[i].state = Wait
  }

  // Monitor Reduce job completion on the ReduceJobChan channel
  reduceDone := make(chan bool)
  go func() {
    for cnt := 0; cnt < mr.nReduce; cnt++ {
      doneJob := <-mr.ReduceJobChan
      mr.ReduceJobs[doneJob].state = "done"
      addr := mr.ReduceJobs[doneJob].worker
      mr.Workers[addr].jobs = append(mr.Workers[addr].jobs, doneJob + 1000)
      mr.Workers[addr].state = "idle"
      DPrintf("RunMaster: Reduce Job %d is done\n", doneJob)
    }
    DPrintf("RunMaster: all Reduce jobs are done\n")
    reduceDone <- true
  }()

  // Assign nReduce jobs to workers
  for jobId := 0; jobId < mr.nReduce; {
    addr := mr.GetIdleWorker()
    if addr == "" {
      continue;
    }
    DPrintf("RunMaster: Assign Reduce job %d to worker %s\n", jobId, addr)
    arg := &DoJobArgs{mr.file, Reduce, jobId, nMap}
    res := &DoJobReply{}
    mr.ReduceJobs[jobId].worker = addr
    mr.ReduceJobs[jobId].state = Assigned
    go func() {
      succ := call(addr, "Worker.DoJob", arg, res)
      if succ {
        mr.ReduceJobChan <- arg.JobNumber
      } else {
        log.Fatalf("Master: failed to execute Reduce Job %d on woker %s", arg.JobNumber, addr)
      }
    }()
    jobId++
  }

  <-reduceDone
  return mr.KillWorkers()
}
