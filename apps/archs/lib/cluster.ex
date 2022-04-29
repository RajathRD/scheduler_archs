defmodule Cluster.Config do
  @enforce_keys [:num_nodes, :cpu_count_per_machine, :memsize_per_machine]
  defstruct(
    num_nodes: nil,
    cpu_count_per_machine: nil,
    memsize_per_machine: nil
  )

  def new(
    num_nodes,
    cpu_count,
    memsize) do
    %Cluster.Config {
      num_nodes: num_nodes,
      cpu_count_per_machine: cpu_count,
      memsize_per_machine: memsize
    }
  end

  def default do
    %Cluster.Config {
      num_nodes: 1,
      cpu_count_per_machine: 10,
      memsize_per_machine: 50
    }
  end
end

defmodule Cluster do
  import Emulation, only: [spawn: 2]

  import Kernel,
  except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  defstruct(
    config: nil,
    nodes: nil
  )

  defp init(config) do
    %Cluster{config: config, nodes: []}
  end

  defp update_nodes(state, nodes) do
    %{state | nodes: state.nodes ++ nodes}
  end

  defp launch_node(name, node_config) do
    spawn(name, fn -> Cluster.Node.start(node_config) end)
  end

  def setup(config) do
    state = init(config)
    nodes = Enum.map(
      1..config.num_nodes,
      fn i -> String.to_atom("n_#{i}") end
    )

    node_config = Cluster.Node.Config.new(
      config.cpu_count_per_machine,
      config.memsize_per_machine
    )

    Enum.map(nodes, fn node -> launch_node(node, node_config) end)
    state = update_nodes(state, nodes)
    :timer.sleep(2_000)

    IO.puts("Cluster setup completed")

    state
  end
end
