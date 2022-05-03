defmodule Cluster.Node do
  import Emulation, only: [send: 2, whoami: 0]

  import Kernel,
  except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  require Logger

  @snapshot_timeout 500

  defstruct(
    resource: nil,
    # FCFS task_queue
    task_queue: nil,
    log: nil,
    trace_log_file: nil,
    resource_log_file: nil,
    snapshot_timeout: nil,
    snapshot_timer: nil,
    snap_count: nil,
    master: nil,
    sync_timeout: nil,
    sync_timer: nil
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

  def init(resource, master \\ nil) do
    me = whoami()
    trace_log_path = "./logs/" <> Atom.to_string(me) <> "_trace.log"
    resource_log_path = "./logs/" <> Atom.to_string(me) <> "_resource.log"
    File.write(trace_log_path, "")
    File.write(resource_log_path, "")
    {status, trace_file} = File.open(trace_log_path, [:write])

    case status do
      :ok ->
        true
      _ ->
        Logger.error("#{me} Log File could not be created at #{trace_log_path}")
    end

    {status, resource_file} = File.open(resource_log_path, [:write])
    case status do
      :ok ->
        true
      _ ->
        Logger.error("#{me} Log File could not be created at #{resource_log_path}")
    end

    %Cluster.Node{
      resource: resource,
      task_queue: nil,
      log: [],
      trace_log_file: trace_file,
      resource_log_file: resource_file,
      snapshot_timeout: @snapshot_timeout,
      snapshot_timer: nil,
      snap_count: 0,
      master: master
    }
  end

  defp start_snapshot_timer(state) do
    %{state | snapshot_timer: Emulation.timer(state.snapshot_timeout, :snap)}
  end

  defp add_snap_count(state) do
    %{state | snap_count: state.snap_count + 1}
  end

  defp stop_snapshot_timer(state) do
    Emulation.cancel_timer(state.snapshot_timer)
    %{state | snapshot_timer: nil}
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

  defp mark_start_time(job) do
    Map.put(job, :start_time, :os.system_time(:milli_seconds))
  end

  defp mark_complete(job) do
    job = Map.put(job, :status, :done)
    Map.put(job, :finish_time, :os.system_time(:milli_seconds))
  end

  defp log_resource(state) do
    me = whoami()
    # IO.puts("#{me} - Logging Resoure")
    log_message = "#{me},#{state.snap_count},#{state.resource.cpu_capacity},#{state.resource.mem_capacity},#{state.resource.cpu_occupied},#{state.resource.mem_occupied}\n"
    IO.write(state.resource_log_file, log_message)
  end

  defp log_job(state, job) do
    me = whoami()
    log_message = "#{job.client},#{me},#{job.scheduler},#{job.id},#{job.arrival_time},#{job.duration},#{job.start_time},#{job.finish_time},#{job.finish_time - job.arrival_time},#{job.finish_time - job.arrival_time - job.duration},#{job.cpu_req},#{job.mem_req}\n"
    IO.write(state.trace_log_file, log_message)
    # IO.puts(log_message)
    %{state | log: state.log ++ [job]}
  end

  defp send_release_rpcs(state, job) do
    me = whoami()
    send(job.scheduler, {
      :release,
      Resource.ReleaseRPC.new(me, job, state.resource)
    })
    if state.master != nil do
      send(state.master, {
        :release,
        Resource.ReleaseRPC.new(me, job, state.resource)
      })
    end
  end

  defp send_jobcreation_rpcs(sender, state, job) do
    me = whoami()
    send(sender, Job.Creation.ReplyRPC.new(me, true, job, state.resource))
    if state.master != nil do
      send(state.master, Job.Creation.ReplyRPC.new(me, true, job.id, state.resource))
    end
  end

  def start(resource, master \\ nil) do
    me = whoami()
    state = init(resource, master)
    IO.puts("Node: #{me} is live")
    state = start_snapshot_timer(state)
    loop(state)
  end

  def loop(state) do
    me = whoami()
    receive do
      {sender, %Job.Creation.RequestRPC{
        scheduler: _,
        job: job
      }} ->

        state = if check_feasibility(state, job) do
          job = mark_start_time(job)
          run_job(job)
          state = occupy(
            state,
            Resource.new(job.cpu_req, job.mem_req)
          )
          send_jobcreation_rpcs(sender, state, job)
          state

        else
          send( sender, Job.Creation.ReplyRPC.new(me, false, job, state.resource))
          state
        end
        # print_resource_state(state)
        loop(state)

      {:done, job} ->
        state = release(
          state,
          Resource.new(job.cpu_req, job.mem_req)
        )

        # IO.puts("Node #{me} - Job #{job.id} Completed. Release: C:#{job.cpu_req} M:#{job.mem_req} ->")
        job = mark_complete(job)
        state = log_job(state, job)

        # print_resource_state(state)
        send_release_rpcs(state, job)
        loop(state)

      :snap ->
        log_resource(state)
        state = add_snap_count(state)
        state = stop_snapshot_timer(state)
        state = start_snapshot_timer(state)
        loop(state)
    end
  end
end
