defmodule Job.Payload do
  @enforce_keys [:id, :arrival_time, :duration, :cpu_req, :mem_req]
  defstruct(
    client: nil,
    scheduler: nil,
    id: nil,
    task_id: nil,
    arrival_time: nil,
    duration: nil,
    finish_time: nil,
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
      scheduler: nil,
      id: job_id,
      task_id: task_id,
      arrival_time: arrival_time,
      duration: duration,
      finish_time: nil,
      cpu_req: cpu_req,
      mem_req: mem_req
    }
  end

  def random(client, job_id) do
      %Job.Payload {
        client: client,
        scheduler: nil,
        id: job_id,
        task_id: 0,
        arrival_time: 0,
        duration: Enum.random(100..400),
        finish_time: nil,
        cpu_req: Enum.random(1..3),
        mem_req: Enum.random(5..10)
      }
  end

  def empty(client, id) do
    %Job.Payload {
      client: client,
      scheduler: nil,
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
    job_id: nil,
    rstate: nil
  )

  def new(node, accept, job_id, rstate) do
    %Job.Creation.ReplyRPC{
      node: node,
      accept: accept,
      job_id: job_id,
      rstate: rstate
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

defmodule Resource.State do
  defstruct(
    cpu_capacity: nil,
    mem_capacity: nil,
    cpu_occupied: nil,
    mem_occupied: nil,
  )

  def new(cpu_cap, mem_cap) do
    %Resource.State{
      cpu_capacity: cpu_cap,
      mem_capacity: mem_cap,
      cpu_occupied: 0,
      mem_occupied: 0,
    }
  end

  def get(cpu_cap, mem_cap, cpu_occ, mem_occ) do
    %Resource.State{
      cpu_capacity: cpu_cap,
      mem_capacity: mem_cap,
      cpu_occupied: cpu_occ,
      mem_occupied: mem_occ,
    }
  end
end

defmodule Resource.ReleaseRPC do
  defstruct(
    node: nil,
    rstate: nil
  )

  def new(node, rstate) do
    %Resource.ReleaseRPC{
      node: node,
      rstate: rstate
    }
  end
end
