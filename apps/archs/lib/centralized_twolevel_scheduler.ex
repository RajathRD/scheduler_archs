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
    all_nodes = cluster.nodes
    all_nodes = Enum.shuffle(all_nodes)

    nodes = %{}

    # split
    nodes = Enum.map(
      0..num_schedulers-1,
      fn i ->
          scheduler = Enum.at(schedulers, i)
          sample_nodes = Enum.slice(all_nodes, i*num_nodes_per_scheduler, num_nodes_per_scheduler)
          %{scheduler => sample_nodes}
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
        scheduler_nodes = Map.get(state.nodes, scheduler)
        num_nodes = length(scheduler_nodes)
        scheduler_cluster = Cluster.Config.new(
          num_nodes,
          state.cluster.cpu_count_per_machine,
          state.cluster.memsize_per_machine,
          scheduler_nodes
        )
        spawn(scheduler, fn -> Scheduler.start(scheduler_cluster) end)
      end
    )

    IO.puts("Scheduler Setup Done")

    :timer.sleep(2000)
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
