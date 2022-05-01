defmodule Reader.Config do
  @enforce_keys []
  defstruct(
    scaling_factor_time: 1,
    scaling_factor_cpu: 10000,
    scaling_factor_mem: 15000,
    trace_path: "resources/trace.csv",
    schedulers: []
  )
end

defmodule Reader do

  import Kernel, except: [send: 2]
  import Emulation, only: [send: 2, whoami: 0]

  @moduledoc false

  require Logger

  def process(row, start_time, config) do
    y = elem(row, 1)
    scaling_factor_cpu = config.scaling_factor_cpu
    scaling_factor_mem = config.scaling_factor_mem
    scaling_factor_time = config.scaling_factor_time
    IO.inspect(y)

    job_id = Enum.fetch(y, 0)
             |> elem(1)
             |> Integer.parse
             |> elem(0)

    task_id = Enum.fetch(y, 1)
              |> elem(1)
              |> Integer.parse
              |> elem(0)

    arrival_time = Enum.fetch(y, 2)
                   |> elem(1)
                   |> Integer.parse
                   |> elem(0)
                   |> Kernel./(1000) # microsecond to millisecond
                   |> Kernel./(scaling_factor_time)
                   |> trunc

    cpus = (scaling_factor_cpu * (Enum.fetch(y, 3)
                                  |> elem(1)
                                  |> Float.parse
                                  |> elem(0)))
           |> trunc

    memory = (scaling_factor_mem * (Enum.fetch(y, 4)
                                    |> elem(1)
                                    |> Float.parse
                                    |> elem(0)))
             |> trunc

    run_time = Enum.fetch(y, 5)
               |> elem(1)
               |> Integer.parse
               |> elem(0)
               |> Kernel./(1000) # microsecond to millisecond
               |> Kernel./(scaling_factor_time)
               |> trunc


    payload = Job.Payload.new("", job_id, task_id, arrival_time, run_time, cpus, memory)

    delay = _get_delay(start_time, arrival_time)
    IO.puts("delay #{delay}")
    #    If delay is too great, hold off processing for a bit

    if delay > 30000 do
      IO.puts("Delay is too large, waiting for a while")
      Process.sleep(delay - 25000)
      delay = _get_delay(start_time, arrival_time)
    end

    submit(payload, delay, config.schedulers)
  end

  def _get_delay(start_time, arrival_time) do
    system_time = DateTime.utc_now()
                  |> DateTime.to_unix(:millisecond)
    current_exec_time = system_time - start_time
    delay = max(arrival_time - current_exec_time, 0)
            |> trunc
    delay
  end

  def submit(payload, delay, schedulers) do
    spawn(
      fn ->
        pid = whoami()

        Logger.debug("Process #{pid}, adding delay of #{delay} to payload #{payload}")

        Process.sleep(delay)

        Logger.debug("Process #{pid}, woke up after delay #{delay}, sending #{payload}")

        sch = Enum.random(schedulers)
        payload = %{payload | scheduler: sch, client: pid}
        IO.inspect(payload)
        send(sch, {:job_submit, payload})
      end
    )
  end

  def read(config) do
    IO.puts("Start reading")
    start_time = DateTime.utc_now()
                 |> DateTime.to_unix(:millisecond)

    File.stream!(config.trace_path)
    |> CSV.decode
      #    |> Enum.take(1000)
    |> Stream.each(
         fn item ->
           process(item, start_time, config)
         end
       )
    |> Stream.run
  end
end