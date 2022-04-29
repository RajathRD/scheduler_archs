defmodule Scheduler.Centralized do
  import Emulation, only: [send: 2, whoami: 0, timer: 2]

  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  defstruct(
    task_queue: nil,
    job_complete_count: nil,
    cluster: nil,
    schedule_timeout: nil,
    retry_timeout: nil,
    schedule_timer: nil,
    retry_timer: nil
  )

  @schedule_timeout 50
  @retry_timeout 300

  def init(cluster) do
    %Scheduler.Centralized{
      task_queue: :queue.new(),
      job_complete_count: 0,
      cluster: cluster,
      schedule_timeout: @schedule_timeout,
      retry_timeout: @retry_timeout,
      schedule_timer: nil,
      retry_timer: nil
    }
  end

  def print_queue(state) do
    IO.puts("Queue: #{inspect(state.task_queue)}")
  end

  def job_complete(state) do
    %{state | job_complete_count: state.job_complete_count + 1}
  end

  def add_job(state, job) do
    %{state | task_queue: :queue.in(job, state.task_queue)}
  end

  def remove_job(state) do
    {{:value, _}, queue} = :queue.out(state.task_queue)
    %{state | task_queue: queue}
  end

  def start_schedule_timer(state) do
    %{state | schedule_timer: Emulation.timer(state.schedule_timeout, :schedule)}
  end

  def stop_schedule_timer(state) do
    Emulation.cancel_timer(state.schedule_timer)
    %{state | schedule_timer: nil}
  end

  def start_schedule_timer(state) do
    %{state | retry_timer: Emulation.timer(state.retry_timeout, :retry_creation)}
  end

  def stop_schedule_timer(state) do
    Emulation.cancel_timer(state.retry_timer)
    %{state | retry_timer: nil}
  end

  defp next_schedule(state) do
    me = whoami()
    if :queue.len(state.task_queue) > 0 do
      job = :queue.get(state.task_queue)
      node = Enum.random(state.cluster.nodes)
      IO.puts("#{me} sent to #{node} job -> CPU: #{job.cpu_req} MEM: #{job.mem_req} Duration:#{job.duration}")
      send(
        node,
        Job.Creation.RequestRPC.new(
          me,
          job
        )
      )
    end
  end

  def start(cluster) do
    state = init(cluster)
    state = start_schedule_timer(state)
    run(state)
  end

  def run(state) do
    me = whoami()
    receive do
      {sender, {:job_submit, job}} ->
        # IO.puts("#{me} received job from client #{sender}")
        state = add_job(state, job)
        run(state)

      {sender, %Job.Creation.ReplyRPC{
        node: node,
        accept: accept,
        job_id: job_id
      }} ->
        state = if accept do
            IO.puts("Node: #{node} - creation success for #{job_id}")
            remove_job(state)
          else
            IO.puts("Node: #{node} - creation failure for #{job_id}")
            state
        end
        state = start_schedule_timer(state)
        run(state)

      :schedule ->
        next_schedule(state)
        state = stop_schedule_timer(state)
        run(state)

    end
  end
end
