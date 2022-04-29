defmodule Coordinator.CentralizedTwoLevel do
  import Emulation, only: [spawn: 2]

  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  defstruct(
    cluster: nil,
    nodes: nil,
    schedulers: nil
  )

  def init_config(cluster, num_schedulers) do
    num_nodes_per_scheduler = ceil(length(cluster.nodes)/num_schedulers)
    schedulers = Enum.map(
      1..num_schedulers,
      fn i -> String.to_atom("s_#{i}") end)
    rem_nodes = cluster.nodes
    nodes = %{}

    # split
    nodes = Enum.map(
      schedulers,
      fn
        scheduler ->
          sample_nodes = Enum.take_random(rem_nodes, num_nodes_per_scheduler)
          rem_nodes = Enum.filter(rem_nodes, fn x -> not Enum.member?(sample_nodes, x) end)
          nodes = Map.put(nodes, scheduler, sample_nodes)
      end)
      |> Enum.reduce(fn x, y -> Map.merge(x, y) end)
    %Coordinator.CentralizedTwoLevel{
      cluster: cluster,
      nodes: nodes,
      schedulers:  schedulers
    }
  end

  def setup_scheduler(state) do
    IO.puts("Coordinator: #{inspect(state)}")

    IO.puts("Schedulers: #{inspect(state.schedulers)}")

    Enum.map(state.schedulers,
      fn scheduler ->
        IO.puts("#{scheduler}: #{inspect(Map.get(state.nodes, scheduler))}")
        spawn(scheduler, fn -> Scheduler.CentralizedTwoLevel.start(Map.get(state.nodes, scheduler)) end)
      end
    )

    IO.puts("Scheduler Setup Done")

    :timer.sleep(1000)
  end

  def start(state) do
    setup_scheduler(state)
    loop(state)
  end

  def loop(state) do
    loop(state)
    receive do
      _ ->
        true
    end
  end
end

defmodule Scheduler.CentralizedTwoLevel do
  import Emulation, only: [send: 2, whoami: 0, timer: 2]

  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  defstruct(
    task_queue: nil,
    job_complete_count: nil,
    nodes: nil,
    schedule_timeout: nil,
    retry_timeout: nil,
    schedule_timer: nil,
    retry_timer: nil
  )

  @schedule_timeout 50
  @retry_timeout 300

  def init(nodes) do
    %Scheduler.CentralizedTwoLevel{
      task_queue: :queue.new(),
      job_complete_count: 0,
      nodes: nodes,
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
      node = Enum.random(state.nodes)
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

  def start(nodes) do
    me = whoami()
    state = init(nodes)
    state = start_schedule_timer(state)
    IO.puts("Scheduler #{me} live: #{inspect(state)}")
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
