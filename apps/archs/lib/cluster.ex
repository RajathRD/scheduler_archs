defmodule Cluster.Config do
  @enforce_keys [:num_nodes, :cpu_count_per_machine, :memsize_per_machine]
  defstruct(
    num_nodes: nil,
    cpu_count_per_machine: nil,
    memsize_per_machine: nil,
    nodes: nil
  )

  def new(
    num_nodes,
    cpu_count,
    memsize,
    nodes) do
    %Cluster.Config {
      num_nodes: num_nodes,
      cpu_count_per_machine: cpu_count,
      memsize_per_machine: memsize,
      nodes: nodes
    }
  end

  def default do
    %Cluster.Config {
      num_nodes: 1,
      cpu_count_per_machine: 10,
      memsize_per_machine: 25,
      nodes: []
    }
  end

  def default_twolevel do
    %Cluster.Config {
      num_nodes: 4,
      cpu_count_per_machine: 10,
      memsize_per_machine: 25,
      nodes: nil
    }
  end
end

defmodule Cluster do
  import Emulation, only: [spawn: 2]

  import Kernel,
  except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  defp update_nodes(state, nodes) do
    %{state | nodes: nodes}
  end

  defp launch_node(name, node_state) do
    spawn(name, fn -> Cluster.Node.start(node_state.resource) end)
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

    Enum.map(Map.keys(nodes), fn node -> launch_node(node, Map.get(nodes, node)) end)
    state = update_nodes(state, nodes)
    :timer.sleep(2_000)

    IO.puts("Cluster setup completed")

    state
  end
end
