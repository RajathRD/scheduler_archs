defmodule Client do
  import Emulation, only: [send: 2, whoami: 0]

  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  defstruct(
    schedulers: [],
  )

  def init(schedulers) do
    %Client{schedulers: schedulers}
  end

  def start(schedulers) do
    state = init(schedulers)
    run(state)
  end

  def run(state) do
    me = whoami()
    job = Job.Payload.empty(me)
    sch = Enum.random(state.schedulers)
    IO.puts("#{me} sending job to #{sch}");
    send(sch, {:job_submit, job})
  end
end
