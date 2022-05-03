defmodule Coordinator.SharedState do
  import Emulation, only: [spawn: 2]

  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  defstruct(
    cluster: nil,
    schedulers: nil
  )

  def init_config(cluster, num_schedulers) do
    schedulers = Enum.map(
      1..num_schedulers,
      fn i -> String.to_atom("s_#{i}") end)

    %Coordinator.SharedState{
      cluster: cluster,
      schedulers:  schedulers
    }
  end

  def setup_scheduler(state) do
    IO.puts("Coordinator: #{inspect(state)}")
    IO.puts("Schedulers: #{inspect(state.schedulers)}")

    Enum.map(state.schedulers,
      fn scheduler ->
        spawn(scheduler, fn -> Scheduler.DRF.start(state.cluster) end)
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
