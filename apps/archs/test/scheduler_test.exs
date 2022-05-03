defmodule SCHTest do
  use ExUnit.Case
  doctest Client
  doctest Scheduler
  doctest Cluster
  doctest Cluster.Node

  @test_timeout 4_000

  import Emulation, only: [spawn: 2]
  import Reader
  import Kernel,
         except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  # test "Run Centralized Scheduler" do
  #   Emulation.init()
  #   cluster_config = Cluster.Config.default()

  #   cluster = Cluster.setup(cluster_config)

  #   spawn(:s_1, fn -> Scheduler.start(cluster) end)
  #   client = spawn(:client_1, fn -> Client.start([:s_1]) end)

  #   Process.send_after(self(), :timeout, @test_timeout)
  #   # Timeout.
  #   receive do
  #     :timeout -> assert true
  #   end
  # after
  #   Emulation.terminate()
  # end


  # test "Run Centralized Two Level Setup Test" do
  #   Emulation.init()
  #   cluster_config = Cluster.Config.default_twolevel()

  #   cluster_state = Cluster.setup(cluster_config)

  #   sched_state = Coordinator.CentralizedTwoLevel.init_config(cluster_state, 4)

  #   coordinator = spawn(:coord_1, fn -> Coordinator.CentralizedTwoLevel.start(sched_state) end)

  #   client = spawn(:client_1, fn -> Client.start(sched_state.schedulers) end)

  #   Process.send_after(self(), :timeout, @test_timeout)
  #   # Timeout.
  #   receive do
  #     :timeout -> assert true
  #   end
  # after
  #   Emulation.terminate()
  # end

  test "Run Shared State Scheduelr Test" do
    Emulation.init()
    cluster_config = Cluster.Config.default_shared()

    cluster_state = Cluster.setup(cluster_config)

    sched_state = Coordinator.SharedState.init_config(cluster_state, 4)

    coordinator = spawn(:coord_1, fn -> Coordinator.SharedState.start(sched_state) end)

    client = spawn(:client_1, fn -> Client.start(sched_state.schedulers) end)

    Process.send_after(self(), :timeout, @test_timeout)

    receive do
      :timeout -> assert true
    end
  after
    Emulation.terminate()
  end

#  @tag timeout: :infinity
#  test "Run Centralized Scheduler with Reader" do
#    Emulation.init()
#    cluster_config = %Cluster.Config{
#      num_nodes: 500,
#      cpu_count_per_machine: 50,
#      memsize_per_machine: 200,
#      nodes: nil
#    }
#    cluster = Cluster.setup(cluster_config)
#    spawn(:s_1, fn -> Scheduler.start(cluster) end)
#
#    reader_config = %Reader.Config{
#      scaling_factor_cpu: 50,
#      scaling_factor_mem: 200,
#      scaling_factor_time: 1000,
#      schedulers: [:s_1],
#      #TODO: Handle better
#      trace_path: "#{Path.expand("../../../", __DIR__)}/resources/trace.csv"
#    }
#    Reader.read(reader_config)
#    Process.send_after(self(), :timeout, @test_timeout)
#    # Timeout.
#    receive do
#      :timeout -> assert true
#    end
#  after
#    Emulation.terminate()
#  end
end
