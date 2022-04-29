defmodule Cluster.Node.Config do
  defstruct(
    cpu_count: nil,
    memsize: nil
  )

  def new(cpu_count, memsize) do
    %Cluster.Node.Config{
      cpu_count: cpu_count,
      memsize: memsize
    }
  end
end

defmodule Cluster.Node do
  import Emulation, only: [send: 2, timer: 2, cancel_timer: 1, whoami: 0]

  import Kernel,
  except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  defstruct(
    cpu_capacity: nil,
    mem_capacity: nil,
    cpu_occupied: nil,
    mem_occupied: nil,
    # FCFS task_queue
    task_queue: nil
  )

  def init(nc) do
    %Cluster.Node{
      cpu_capacity: nc.cpu_count,
      mem_capacity: nc.memsize,
      cpu_occupied: 0,
      mem_occupied: 0,
      task_queue: :queue.new()
    }
  end

  def start(config) do
    me = whoami()
    state = init(config)
    IO.puts("Node: #{me} is live")
    loop(state)
  end

  def add_occupancy(state, job) do
    %{state |
      cpu_occupied: state.cpu_occupied + job.cpu_req,
      mem_occupied: state.mem_occupied + job.mem_req
    }
  end

  def occupy(state, resource) do
    %{state |
      cpu_occupied: state.cpu_occupied + resource.cpu,
      mem_occupied: state.mem_occupied + resource.mem
    }
  end

  def release(state, resource) do
    %{state |
      cpu_occupied: state.cpu_occupied - resource.cpu,
      mem_occupied: state.mem_occupied - resource.mem
    }
  end

  def check_feasibility(state, job) do
    if state.cpu_occupied + job.cpu_req <= state.cpu_capacity and
      state.mem_occupied + job.mem_req <= state.mem_capacity do
      true
    else
      false
    end
  end

  def run_job(job) do
    Process.send_after(
      self(),
      {:release, Resource.new(job.cpu_req, job.mem_req)},
      job.duration
    )
  end

  def loop(state) do
    me = whoami()
    receive do
      {sender, %Job.Creation.RequestRPC{
        scheduler: scheduler,
        job: job
      }} ->
        IO.puts("Node: #{me} -> #{inspect(state)}")
        state = if check_feasibility(state, job) do
          send(sender, Job.Creation.ReplyRPC.new(me, true, job.id))
          run_job(job)
          state = occupy(
            state,
            Resource.new(job.cpu_req, job.mem_req)
          )
        else
          send(sender, Job.Creation.ReplyRPC.new(me, false, job.id))
          state
        end

        loop(state)

      {:release, %Resource{
        cpu: cpu,
        mem: mem}} ->
          IO.puts("Node: #{me} --> Release resource #{cpu} and #{mem}")
          state = release(
            state,
            Resource.new(cpu, mem)
          )
          loop(state)

        msg ->
          IO.puts("Node: #{me} received #{inspect(msg)}")
    end
  end
end
