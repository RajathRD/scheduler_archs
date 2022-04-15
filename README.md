Use vagrant up to create VM for project

RUN on root directory

mix deps.get
mix compile
mix test test/sch_test.exs:5 (line number for test)