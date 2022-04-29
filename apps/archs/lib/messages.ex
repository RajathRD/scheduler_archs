defmodule Job.Payload do
  @enforce_keys [:job_id, :arrival_time, :duration, :cpu_req, :mem_req]
  defstruct(
    client: nil,
    job_id: nil,
    task_id: nil,
    arrival_time: nil,
    duration: nil,
    cpu_req: nil,
    mem_req: nil
  )

  def new(
    client,
    job_id,
    task_id,
    arrival_time,
    duration,
    cpu_req,
    mem_req) do
    %Job.Payload {
      client: client,
      job_id: job_id,
      task_id: task_id,
      arrival_time: arrival_time,
      duration: duration,
      cpu_req: cpu_req,
      mem_req: mem_req
    }
  end

  def empty(client) do
    %Job.Payload {
      client: client,
      job_id: 0,
      task_id: 0,
      arrival_time: 0,
      duration: 10,
      cpu_req: 2,
      mem_req: 2
    }
  end
end
