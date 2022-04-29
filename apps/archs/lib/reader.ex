defmodule Reader do
  import Kernel, except: [if: 2, unless: 2]

  @moduledoc false

  #TODOS -
  # Filter and publish everything that arrives at time 0 first.
  # Post this, track system time and send message to scheduler with details when arrival_time >= current_time - start_time. Else wait.
  # To accomodate time scaling, just use a factor to divide arrival time and then publish events as needed.
  def read do
    File.stream!('resources/trace.csv')
    |> CSV.decode
      # Remove ->
    |> Enum.take(1)
    |> Stream.each(
         fn x ->
           y = elem(x, 1)
           scaling_factor_cpu = 10000
           scaling_factor_mem = 15000

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

           IO.puts("#{job_id} - #{task_id} - #{arrival_time} - #{cpus} - #{memory} - #{run_time}")

         end
       )
    |> Stream.run
  end

end