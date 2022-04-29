defmodule Cluster.Node.Config do
  defstruct(
    cpu_count: nil,
    memsize: nil,
  )

  def new(cpu_count, memsize) do
    %Cluster.Node.Config{
      cpu_count: cpu_count,
      memsize: memsize,
    }
  end
end

defmodule Cluster.Node do
  import Emulation, only: [timer: 2, cancel_timer: 1, whoami: 0]

  import Kernel,
  except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  defstruct(
    cpu_count: nil,
    memsize: nil,
    # FCFS task_queue
    task_queue: nil
  )

  def init(c) do
    %Cluster.Node{
      cpu_count: c.cpu_count,
      memsize: c.memsize,
      task_queue: :queue.new()
    }
  end

  def start(config) do
    me = whoami()
    state = init(config)
    IO.puts("Node: #{me} is live")
    loop(state)
  end

  def loop(state) do
    receive do
      _ ->
        loop(state)
    end
  end
end
