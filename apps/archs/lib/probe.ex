defmodule Probe do
  import Emulation, only: [send: 2, whoami: 0, timer: 2]

  @probe_timeout 2_000

  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  defstruct(
    cluster: nil,
    timeout: nil,
    slack: nil,
    util_log_file: nil
  )

  # def init(cluster) do
  #   me = whoami()
  #   log_path = "./logs/" <> Atom.to_string(me) <> ".log"
  #   File.write(log_path, "")
  #   %Probe{
  #     cluster: cluster,
  #     timeout: @probe_timeout,
  #     slack: @probe_timeout + 2_000,
  #     util_log_file:
  #   }
  # end

  # def start(cluster) do
  #   state = init(cluster)
  #   run(state)
  # end

  # def run(state) do
  #   receive do
  #     :util_timeout ->

  #     {sender, %Probe.ResourceState{
  #       node: node,
  #       rstate: rstate,
  #       time: time
  #     }} ->

  # end
end
