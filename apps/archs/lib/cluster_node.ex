# defmodule Cluster.Node.Config do
#   defstruct(
#     cpu_count: nil,
#     memsize: nil
#   )

#   def new(cpu_count, memsize) do
#     %Cluster.Node.Config{
#       cpu_count: cpu_count,
#       memsize: memsize
#     }
#   end
# end


defmodule Cluster.Node do
  import Emulation, only: [send: 2, whoami: 0]

  import Kernel,
  except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  defstruct(
    resource: nil,
    # FCFS task_queue
    task_queue: nil,
    log: nil
  )

  def print_resource_state(state) do
    me = whoami()
    IO.puts("Node: #{me} ->
      CPU CAP: #{state.resource.cpu_capacity}
      CPU USE: #{state.resource.cpu_occupied}
      MEM CAP: #{state.resource.mem_capacity}
      MEM USE: #{state.resource.mem_occupied}")
    state
  end

  def init(resource) do
    %Cluster.Node{
      resource: resource,
      task_queue: nil,
      log: []
    }
  end

  def start(resource) do
    me = whoami()
    state = init(resource)
    IO.puts("Node: #{me} is live")
    loop(state)
  end

  def occupy(state, resource) do
    %{state | resource:
      Resource.State.get(
        state.resource.cpu_capacity,
        state.resource.mem_capacity,
        state.resource.cpu_occupied + resource.cpu,
        state.resource.mem_occupied + resource.mem
      )
    }
  end

  def release(state, resource) do
    %{state | resource:
      Resource.State.get(
        state.resource.cpu_capacity,
        state.resource.mem_capacity,
        state.resource.cpu_occupied - resource.cpu,
        state.resource.mem_occupied - resource.mem
      )
    }
  end

  def check_feasibility(state, job) do
    if state.resource.cpu_occupied + job.cpu_req <= state.resource.cpu_capacity and
      state.resource.mem_occupied + job.mem_req <= state.resource.mem_capacity do
      true
    else
      false
    end
  end

  def run_job(job) do
    Process.send_after(
      self(),
      {:done, job},
      job.duration
    )
  end

  defp mark_complete(job) do
    job = Map.put(job, :status, :done)
    Map.put(job, :finish_time, :os.system_time(:milli_seconds))
  end

  defp log_job(state, job) do
    %{state | log: state.log ++ [job]}
  end

  def loop(state) do
    me = whoami()
    receive do
      {sender, %Job.Creation.RequestRPC{
        scheduler: _,
        job: job
      }} ->

        state = if check_feasibility(state, job) do
          run_job(job)
          state = occupy(
            state,
            Resource.new(job.cpu_req, job.mem_req)
          )
          send(sender, Job.Creation.ReplyRPC.new(me, true, job.id, state.resource))
          state

        else
          send(sender, Job.Creation.ReplyRPC.new(me, false, job.id, state.resource))
          state
        end
        print_resource_state(state)
        loop(state)

      {:done, job} ->
        state = release(
          state,
          Resource.new(job.cpu_req, job.mem_req)
        )

        IO.puts("Node #{me} - Job #{job.id} Completed. Release: C:#{job.cpu_req} M:#{job.mem_req} ->")
        job = mark_complete(job)
        state = log_job(state, job)

        print_resource_state(state)

        send(job.scheduler, {
          :release,
          Resource.ReleaseRPC.new(me, state.resource)
        })

        loop(state)
    end
  end
end
