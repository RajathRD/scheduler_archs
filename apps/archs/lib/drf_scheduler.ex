defmodule Scheduler.DRF do
  import Emulation, only: [send: 2, whoami: 0, timer: 2]

  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  @sync_timeout 300
  @schedule_timeout 50
  @retry_timeout 300

  defstruct(
    task_queue: nil,
    job_complete_count: nil,
    cluster: nil,
    schedule_timeout: nil,
    retry_timeout: nil,
    sync_timeout: nil,
    schedule_timer: nil,
    retry_timer: nil,
    sync_timer: nil,
    dom_resource: nil,
    user_alloc: nil
  )

  def init(cluster) do
    %Scheduler.DRF{
      task_queue: %{},
      job_complete_count: 0,
      cluster: cluster,
      schedule_timeout: @schedule_timeout,
      retry_timeout: @retry_timeout,
      sync_timeout: @sync_timeout,
      schedule_timer: nil,
      retry_timer: nil,
      sync_timer: nil,
      dom_resource: %{},
      user_alloc: %{}
    }
  end

  def print_queue(state) do
    IO.puts("Queue: #{inspect(state.task_queue)}")
  end

  def job_complete(state) do
    %{state | job_complete_count: state.job_complete_count + 1}
  end

  def get_empty_user_alloc(state) do
    Enum.map(Map.keys(state.cluster.nodes), fn node -> %{node => Resource.empty()} end)
    |> Enum.reduce(fn x,y -> Map.merge(x, y) end)
  end

  def handle_new_user(state, job) do
    if not Map.has_key?(state.task_queue, job.client) do
      state = %{state |
        task_queue: Map.put(
          state.task_queue,
          job.client, :queue.new())
      }
      state = %{state |
        dom_resource: Map.put(state.dom_resource, job.client, 0)
      }

      %{state | user_alloc: Map.put(state.user_alloc, job.client, get_empty_user_alloc(state))}
    else
      state
    end

  end

  def add_job(state, job) do
    me = whoami()
    job = Map.put(job, :scheduler, me)
    job = Map.put(job, :arrival_time, :os.system_time(:milli_seconds))

    %{state |
      task_queue: Map.put(
        state.task_queue,
        job.client, :queue.in(
          job,
          Map.get(state.task_queue, job.client))
      )
    }
  end

  def remove_job(state, client) do
    {{:value, _}, queue} = :queue.out(state.task_queue[client])
    # %{state | task_queue: queue}
    %{state | task_queue:
      %{state.task_queue | client => queue}
    }
  end

  defp update_user_alloc(state, node, job, type) do
    # IO.puts("#{node}, #{job}, #{type}")
    cpu_req = job.cpu_req
    mem_req = job.mem_req
    client = job.client
    resource = state.user_alloc[client][node]

    resource = if type == :occupy do
      Resource.new(
        resource.cpu + cpu_req,
        resource.mem + mem_req
      )
    else
      Resource.new(
        resource.cpu - cpu_req,
        resource.mem - mem_req
      )
    end

    %{state | user_alloc:
      %{state.user_alloc |
        client => Map.put(state.user_alloc[client], node, resource)
      }
    }
  end

  def update_node_state(state, node, rstate) do
    %{state | cluster: %{
      state.cluster | nodes: Map.put(state.cluster.nodes, node, %{resource: rstate})
      }
    }
  end

  def update_node_state(state, nodes) do
    %{state | cluster: Map.put(state.cluster, :nodes, nodes)}
  end

  defp start_sync_timer(state) do
    %{state | sync_timer: Emulation.timer(state.sync_timeout, :sync_rstate)}
  end

  defp stop_sync_timer(state) do
    Emulation.cancel_timer(state.sync_timer)
    %{state | sync_timer: nil}
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

  defp calc_dom_share(state, users) do
    # IO.puts("Calculate Dom Share for: #{inspect(users)}")
    # IO.puts("#{inspect(state.user_alloc)}")
    total_alloc = Enum.map(
      users,
      fn user ->
        alloc = state.user_alloc[user]
        nodes = Map.keys(state.user_alloc[user])
        # IO.puts("#{user}\nAlloc:#{inspect(alloc)}\nNodes:#{inspect(nodes)}}")
        total_alloc_user = if length(nodes) > 1 do
          Resource.new(
            Enum.map(nodes, fn node -> alloc[node].cpu end) |> Enum.sum(),
            Enum.map(nodes, fn node -> alloc[node].mem end) |> Enum.sum()
          )
        else
          alloc[Enum.at(nodes, 0)]
        end

        # IO.puts("Total Alloc:#{inspect(total_alloc_user)}")
        # IO.puts("#{state.cluster.cluster_cpu_count}, #{state.cluster.cluster_memsize}")
        max(total_alloc_user.cpu/state.cluster.cluster_cpu_count, total_alloc_user.mem/state.cluster.cluster_memsize)
      end
    )

  end

  defp next_schedule(state) do
    me = whoami()
    users = Map.keys(state.task_queue)

    if length(users) > 0 do
      pending_users = Enum.filter(users, fn user -> :queue.len(state.task_queue[user]) > 0 end)


      if length(pending_users) > 0 do
        dom_shares = calc_dom_share(state, pending_users)
        min_index = Enum.min_by(0..length(dom_shares), fn i -> Enum.at(dom_shares, i) end)
        # IO.puts("#{inspect(state.user_alloc)}")
        # IO.puts("#{inspect(pending_users)}: #{inspect(dom_shares)}")

        # user = Enum.random(pending_users)
        user = Enum.at(pending_users, min_index)
        if :queue.len(state.task_queue[user]) > 0 do
          job = :queue.get(state.task_queue[user])
          # node = Enum.random(state.cluster.nodes)
          available_nodes = Enum.filter(Map.keys(state.cluster.nodes), fn node ->
            node_state = Map.get(state.cluster.nodes, node)
            check_feasibility(node_state.resource, job) end)

          if length(available_nodes) > 0 do
            node = Enum.random(available_nodes)
            # IO.puts("#{me} sent to #{node} job -> CPU: #{job.cpu_req} MEM: #{job.mem_req} Duration:#{job.duration}")
            send(
              node,
              Job.Creation.RequestRPC.new(
                me,
                job
              )
            )
            true
          else
            # IO.puts("No Avalable Nodes")
            false
          end
        else
          false
        end
      else
        false
      end
    else
      false
    end
  end

  def start(cluster) do
    me = whoami()
    state = init(cluster)
    state = start_schedule_timer(state)

    state = if state.cluster.master != nil do
        start_sync_timer(state)
      else
        state
      end
    run(state)
  end

  def run(state) do
    me = whoami()
    receive do
      {sender, {:job_submit, job}} ->
        # IO.puts("#{me} received job from client #{sender}")
        state = handle_new_user(state, job)
        state = add_job(state, job)
        # print_queue(state)
        run(state)

      {sender, %Job.Creation.ReplyRPC{
        node: node,
        accept: accept,
        job: job,
        rstate: rstate
      }} ->
        state = if accept do
            # IO.puts("Node: #{node} - creation success for #{job.id} -> #{inspect(rstate)}")
            state = update_node_state(state, node, rstate)
            state = update_user_alloc(state, node, job, :occupy)
            # IO.puts("Create: #{job.client} #{inspect(state.user_alloc)}")
            remove_job(state, job.client)
          else
            # IO.puts("Node: #{node} - creation failure for #{job.id}")
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

      {sender, {:release, %Resource.ReleaseRPC{
        node: node,
        job: job,
        rstate: rstate
      }}} ->
        state = update_node_state(state, node, rstate)
        state = update_user_alloc(state, node, job, :release)
        # IO.puts("Release: #{job.client} #{inspect(state.user_alloc)}")
        # IO.puts("#{me} received ReleaseRPC from #{node}")
        run(state)

      :sync_rstate ->
        # IO.puts("trying to sync with #{state.cluster.master}...")
        send(
          state.cluster.master,
          {:sync_rstate, Resource.Synchronize.RequestRPC.new(me)}
        )
        state = stop_sync_timer(state)
        state = start_sync_timer(state)
        run(state)

      {sender, {:sync_rstate, %Resource.Synchronize.ReplyRPC{
        nodes: nodes
      }}} ->
        state = update_node_state(state, nodes)
        run(state)
    end
  end
end
