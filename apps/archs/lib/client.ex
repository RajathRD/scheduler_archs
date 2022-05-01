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
    # infinite_submit(state, 0)
    submit_njobs(state, 0, 5)
  end

  def infinite_submit(state, id) do
    me = whoami()

    # job = Job.Payload.empty(me, id)

    :timer.sleep(state.timeout)
    job = Job.Payload.random(me, id)

    sch = Enum.random(state.schedulers)
    # IO.puts("#{me} sending job to #{sch}");
    send(sch, {:job_submit, job})

    infinite_submit(state, id+1)
  end

  def submit_njobs(state, id, n) do
    me = whoami()

    :timer.sleep(state.timeout)
    job = Job.Payload.random(me, id)

    sch = Enum.random(state.schedulers)
    # IO.puts("#{me} sending job to #{sch}");
    send(sch, {:job_submit, job})

    if id < n do
      submit_njobs(state, id+1, n)
    end
  end
end
