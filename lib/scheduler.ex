defmodule Scheduler do
  import Utils, only: [spawn: 2, send_: 2]
  @moduledoc """
    Documentation for `Scheduler`.
  """
  @enforce_keys [:task_queue]
  defstruct(
    task_queue: nil
  )
  @doc """

  """
  def init do
    %Scheduler{
      task_queue: :queue.new()
    }
  end

  def print_queue(state) do
    IO.puts("Queue: #{inspect(state.task_queue)}")
  end

  def add_job(state, job) do
    %{state | task_queue: :queue.in(job, state.task_queue)}
  end

  def start do
    state = init()
    run(state)
  end

  def run(state) do
    # IO.puts("SCH running")
    receive do
      {sender, {:job_arrival, job}} ->
        IO.puts("Received job: #{inspect(job)}")
        state = add_job(state, job)
        print_queue(state)
        run(state)
      _ ->
        run(state)
    end

  end

  def test_run do
    spawn(:server, fn -> Scheduler.start() end)
    send_(:server, {:job_arrival, {100, 5, 8}})
  end
end
