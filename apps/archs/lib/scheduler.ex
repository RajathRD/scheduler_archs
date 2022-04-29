defmodule Scheduler do
  import Emulation, only: [send: 2, whoami: 0]

  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  @enforce_keys [:task_queue]
  defstruct(
    task_queue: nil,
    job_complete_count: nil
  )

  def init do
    %Scheduler{
      task_queue: :queue.new(),
      job_complete_count: 0
    }
  end

  def print_queue(state) do
    IO.puts("Queue: #{inspect(state.task_queue)}")
  end

  def inc_job_complete(state) do
    %{state | job_complete_count: state.job_complete_count + 1}
  end

  def add_job(state, job) do
    %{state | task_queue: :queue.in(job, state.task_queue)}
  end

  def start do
    state = init()
    run(state)
  end

  def run(state) do
    receive do
      {sender, {:job_submit, job}} ->
        IO.puts("#{job.client} submitted job: #{inspect(job)}")
        state = add_job(state, job)
        print_queue(state)
        run(state)
      _ ->
        run(state)
    end
  end
end
