defmodule Client do
  import Emulation, only: [send: 2, whoami: 0]

  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  defstruct(
    schedulers: [],
    timeout: nil
  )

  def init(schedulers) do
    %Client{
      schedulers: schedulers,
      timeout: 20
    }
  end

  def start(schedulers) do
    state = init(schedulers)
    submit(state, 0)
  end

  def submit(state, id) do
    me = whoami()

    # job = Job.Payload.empty(me, id)

    :timer.sleep(state.timeout)
    job = Job.Payload.random(me, id)

    sch = Enum.random(state.schedulers)
    # IO.puts("#{me} sending job to #{sch}");
    send(sch, {:job_submit, job})

    submit(state, id+1)
  end
end
