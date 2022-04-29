defmodule SCHTest do
  use ExUnit.Case
  doctest Client
  doctest Scheduler
  doctest Cluster
  doctest Cluster.Node

  import Emulation, only: [spawn: 2]

  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  test "Run Test" do
    Emulation.init()
    cluster_config = Cluster.Config.default()

    Cluster.setup(cluster_config)

    spawn(:scheduler_1, fn -> Scheduler.start() end)
    client = spawn(:client_1, fn -> Client.start([:scheduler_1]) end)

    Process.send_after(self(), :timeout, 2_000)
    # Timeout.
    receive do
      :timeout -> assert true
    end
  after
    Emulation.terminate()
  end
end
