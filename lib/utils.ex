defmodule Utils do
  @moduledoc """
    Utils that help send and spawning more intuitive
  """

  @doc """
  """
  @spec spawn(
    atom(),
    any()
  ) :: no_return()
  def spawn(atom, func) do
    pid = spawn(func)
    Process.register(pid, atom)
    pid
  end

  def send_(pid, m) do
    send(pid, {self(), m})
  end

end
