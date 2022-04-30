defmodule SCHTest do
  use ExUnit.Case
  doctest Client
  doctest Scheduler
  doctest Cluster
  doctest Cluster.Node

  import Emulation, only: [spawn: 2]
  import Reader
  import Kernel,
         except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  test "Run Centralized Scheduler" do
    Emulation.init()
    cluster_config = Cluster.Config.default()

    cluster = Cluster.setup(cluster_config)

    spawn(:s_1, fn -> Scheduler.start(cluster) end)
    client = spawn(:client_1, fn -> Client.start([:s_1]) end)

    Process.send_after(self(), :timeout, 2_000)
    # Timeout.
    receive do
      :timeout -> assert true
    end
  after
    Emulation.terminate()
  end


  # test "Run Centralized Two Level Setup Test" do
  #   Emulation.init()
  #   cluster_config = Cluster.Config.default_twolevel()

  #   cluster_state = Cluster.setup(cluster_config)

  #   sched_state = Coordinator.CentralizedTwoLevel.init_config(cluster_state, 4)

  #   coordinator = spawn(:coord_1, fn -> Coordinator.CentralizedTwoLevel.start(sched_state) end)

  #   client = spawn(:client_1, fn -> Client.start(sched_state.schedulers) end)

  #   Process.send_after(self(), :timeout, 2_000)
  #   # Timeout.
  #   receive do
  #     :timeout -> assert true
  #   end
  # after
  #   Emulation.terminate()
  # end

  # test "Run Shared State Scheduelr Test" do
  #   Emulation.init()
  #   cluster_config = Cluster.Config.default_twolevel()

  #   cluster_state = Cluster.setup(cluster_config)

  #   sched_state = Coordinator.CentralizedTwoLevel.init_config(cluster_state, 4)

  #   coordinator = spawn(:coord_1, fn -> Coordinator.CentralizedTwoLevel.start(sched_state) end)

  #   client = spawn(:client_1, fn -> Client.start(sched_state.schedulers) end)

  #   Process.send_after(self(), :timeout, 2_000)
  #   # Timeout.
  #   receive do
  #     :timeout -> assert true
  #   end
  # after
  #   Emulation.terminate()
  # end

  test "Run Centralized Scheduler with Reader" do
    Emulation.init()
    cluster_config = Cluster.Config.default()

    cluster = Cluster.setup(cluster_config)
    spawn(:s_1, fn -> Scheduler.start(cluster) end)

    reader_config = %Reader.Config{
      scaling_factor_cpu: 100,
      scaling_factor_mem: 150,
      scaling_factor_time: 1,
      schedulers: [:s_1],
      #TODO: Handle better
      trace_path: "#{Path.expand("../../../", __DIR__)}/resources/trace.csv"
    }
    Reader.read(reader_config)
    Process.send_after(self(), :timeout, 2_000)
    # Timeout.
    receive do
      :timeout -> assert true
    end
  after
    Emulation.terminate()
  end
end
