defmodule Cluster.Config do
  @enforce_keys [:num_nodes, :cpu_count_per_machine, :memsize_per_machine]
  defstruct(
    num_nodes: nil,
    cpu_count_per_machine: nil,
    memsize_per_machine: nil,
    nodes: nil,
    master: nil,
    cluster_cpu_count: nil,
    cluster_memsize: nil
  )

  def new(
    num_nodes,
    cpu_count,
    memsize,
    nodes, master \\ nil) do
    %Cluster.Config {
      num_nodes: num_nodes,
      cpu_count_per_machine: cpu_count,
      memsize_per_machine: memsize,
      nodes: nodes,
      master: master,
      cluster_cpu_count: num_nodes * cpu_count,
      cluster_memsize: num_nodes * memsize
    }
  end

  def default(num_nodes \\ 1, cpu_count \\ 10, memsize \\ 25) do
    %Cluster.Config {
      num_nodes: num_nodes,
      cpu_count_per_machine: cpu_count,
      memsize_per_machine: memsize,
      nodes: nil,
      master: nil,
      cluster_cpu_count: num_nodes * cpu_count,
      cluster_memsize: num_nodes * memsize
    }
  end

  def default_twolevel(num_nodes \\ 4, cpu_count \\ 10, memsize \\ 25) do
    %Cluster.Config {
      num_nodes: num_nodes,
      cpu_count_per_machine: cpu_count,
      memsize_per_machine: memsize,
      nodes: nil,
      master: nil,
      cluster_cpu_count: num_nodes * cpu_count,
      cluster_memsize: num_nodes * memsize
    }
  end

    def default_shared(num_nodes \\ 4, cpu_count \\ 10, memsize \\ 25) do
      %Cluster.Config {
        num_nodes: num_nodes,
        cpu_count_per_machine: cpu_count,
        memsize_per_machine: memsize,
        nodes: nil,
        master: nil,
        cluster_cpu_count: num_nodes * cpu_count,
        cluster_memsize: num_nodes * memsize
      }
  end
end

defmodule Cluster.Master do
  import Emulation, only: [send: 2, whoami: 0]

  import Kernel,
  except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  defp init(config) do
    Cluster.Config.new(
      config.num_nodes,
      config.cpu_count_per_machine,
      config.memsize_per_machine,
      config.nodes
    )
  end

  def update_node_state(state, node, rstate) do
    %{state | nodes: Map.put(state.nodes, node, %{resource: rstate})}
  end

  def print_state(state) do
    IO.puts("#{inspect(state.nodes)}")
  end

  def start(config) do
    me = whoami()
    state = init(config)
    IO.puts("#{me} is live")
    run(state)
  end

  defp run(state) do
    receive do
      {sender, {:sync_rstate, %Resource.Synchronize.RequestRPC{
        scheduler: scheduler
      }}} ->

        # IO.puts("Master Received Sync request from #{scheduler}")
        send(sender, {:sync_rstate, Resource.Synchronize.ReplyRPC.new(state.nodes)})
        run(state)

      {sender, {:release, %Resource.ReleaseRPC{
        node: node,
        job: _,
        rstate: rstate
      }}} ->
        # IO.puts("#{node} release")
        # print_state(state)
        state = update_node_state(state, node, rstate)

        run(state)
      {sender, %Job.Creation.ReplyRPC{
        node: node,
        accept: accept,
        job: _,
        rstate: rstate
      }} ->
        # IO.puts("#{node} occupy")
        state = update_node_state(state, node, rstate)
        # print_state(state)
        run(state)
    end
  end
end

defmodule Cluster do
  import Emulation, only: [spawn: 2]

  import Kernel,
  except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  defp update_cluster_totals(state) do
    state = %{state | cluster_cpu_count: state.num_nodes * state.cpu_count_per_machine}
    %{state | cluster_memsize: state.num_nodes * state.memsize_per_machine}
  end

  defp update_nodes(state, nodes) do
    %{state | nodes: nodes}
  end

  defp launch_node(name, node_state, master) do
    spawn(name, fn -> Cluster.Node.start(node_state.resource, master) end)
  end

  defp launch_master(config) do
    spawn(config.master, fn -> Cluster.Master.start(config) end)
  end

  def setup(config) do
    state = config
    nodes = Enum.map(
      1..state.num_nodes,
      fn i -> %{
        String.to_atom("n_#{i}") =>
        %{
          :resource => Resource.State.new(state.cpu_count_per_machine, state.memsize_per_machine)
        }
      }
      end)
      |> Enum.reduce(fn x, y -> Map.merge(x, y) end)

    Enum.map(Map.keys(nodes), fn node -> launch_node(node, Map.get(nodes, node), state.master) end)
    state = update_nodes(state, nodes)
    :timer.sleep(2_000)

    if config.master != nil do
      launch_master(state)
    end
    IO.puts("Cluster setup completed")

    IO.puts("Cluster Config: #{inspect(state)}")
    state
  end
end
