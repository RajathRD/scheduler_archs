defmodule Scheduler do
  import Emulation, only: [send: 2, whoami: 0, timer: 2]

  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]


  @schedule_timeout 50
  @retry_timeout 300

  defstruct(
    task_queue: nil,
    job_complete_count: nil,
    cluster: nil,
    schedule_timeout: nil,
    retry_timeout: nil,
    schedule_timer: nil,
    retry_timer: nil
  )

  def init(cluster) do
    %Scheduler{
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

  def update_node_state(state, node, rstate) do
    %{state | cluster: %{
      state.cluster | nodes: Map.put(state.cluster.nodes, node, %{resource: rstate})
      }
    }
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

  def check_feasibility(resource, job) do
    if resource.cpu_occupied + job.cpu_req <= resource.cpu_capacity and
      resource.mem_occupied + job.mem_req <= resource.mem_capacity do
      true
    else
      false
    end
  end

  defp next_schedule(state) do
    me = whoami()
    if :queue.len(state.task_queue) > 0 do
      job = :queue.get(state.task_queue)
      # node = Enum.random(state.cluster.nodes)
      available_nodes = Enum.filter(Map.keys(state.cluster.nodes), fn node ->
        node_state = Map.get(state.cluster.nodes, node)
        check_feasibility(node_state.resource, job) end)

      if length(available_nodes) > 0 do
        node = Enum.random(available_nodes)
        IO.puts("#{me} sent to #{node} job -> CPU: #{job.cpu_req} MEM: #{job.mem_req} Duration:#{job.duration}")
        send(
          node,
          Job.Creation.RequestRPC.new(
            me,
            job
          )
        )
        true
      else
        IO.puts("No Avalable Nodes")
        false
      end
    else
      false
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
        job_id: job_id,
        rstate: rstate
      }} ->
        state = if accept do
            IO.puts("Node: #{node} - creation success for #{job_id} -> #{inspect(rstate)}")
            state = update_node_state(state, node, rstate)
            remove_job(state)
          else
            IO.puts("Node: #{node} - creation failure for #{job_id}")
            state
        end
        state = start_schedule_timer(state)
        run(state)

      :schedule ->
        state = if next_schedule(state) do
          stop_schedule_timer(state)
        else
          state = stop_schedule_timer(state)
          start_schedule_timer(state)
        end

        run(state)

    end
  end
end
