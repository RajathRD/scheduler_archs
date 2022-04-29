defmodule Job.Payload do
  @enforce_keys [:id, :arrival_time, :duration, :cpu_req, :mem_req]
  defstruct(
    client: nil,
    id: nil,
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
      id: job_id,
      task_id: task_id,
      arrival_time: arrival_time,
      duration: duration,
      cpu_req: cpu_req,
      mem_req: mem_req
    }
  end

  def random(client, job_id) do
      %Job.Payload {
        client: client,
        id: job_id,
        task_id: 0,
        arrival_time: 0,
        duration: Enum.random(100..400),
        cpu_req: Enum.random(1..3),
        mem_req: Enum.random(5..10)
      }
  end

  def empty(client, id) do
    %Job.Payload {
      client: client,
      id: id,
      task_id: 0,
      arrival_time: 0,
      duration: 50,
      cpu_req: 2,
      mem_req: 2
    }
  end
end

defmodule Job.Creation.RequestRPC do
  defstruct(
    scheduler: nil,
    job: nil
  )

  def new(scheduler, job) do
    %Job.Creation.RequestRPC{
      scheduler: scheduler,
      job: job
    }
  end
end

defmodule Job.Creation.ReplyRPC do
  defstruct(
    node: nil,
    accept: nil,
    job_id: nil
  )

  def new(node, accept, job_id) do
    %Job.Creation.ReplyRPC{
      node: node,
      accept: accept,
      job_id: job_id
    }
  end
end

defmodule Resource do
  defstruct(
    cpu: nil,
    mem: nil
  )

  def new(cpu, mem) do
    %Resource{
      cpu: cpu,
      mem: mem
    }
  end
end
