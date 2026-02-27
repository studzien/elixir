Mix.install([{:benchee, "~> 1.0"}])

defmodule BenchHelper do
  @moduledoc false

  def spawn_worker do
    caller = self()
    spawn(fn -> worker_loop(caller) end)
  end

  defp worker_loop(caller) do
    receive do
      {:register, registry, key, value, ref} ->
        result = Registry.register(registry, key, value)
        send(caller, {:ok, ref, result})
        worker_loop(caller)

      {:unregister, registry, key, ref} ->
        result = Registry.unregister(registry, key)
        send(caller, {:ok, ref, result})
        worker_loop(caller)

      :stop ->
        :ok
    end
  end

  def register_worker(pid, registry, key, value \\ :ok) do
    ref = make_ref()
    send(pid, {:register, registry, key, value, ref})

    receive do
      {:ok, ^ref, {:ok, _}} -> :ok
    end
  end

  def unregister_worker(pid, registry, key) do
    ref = make_ref()
    send(pid, {:unregister, registry, key, ref})

    receive do
      {:ok, ^ref, :ok} -> :ok
    end
  end

  def stop_worker(pid) do
    send(pid, :stop)
  end

  def populate(registry, key, n) do
    batch_size = min(n, 1000)
    workers = for _ <- 1..batch_size, do: spawn_worker()

    for i <- 1..n do
      pid = Enum.at(workers, rem(i - 1, batch_size))
      register_worker(pid, registry, key)
    end

    workers
  end

  def sync_registry(registry) do
    [{-1, {_, partitions, _key_ets, pid_ets, _listeners}}] =
      :ets.lookup(registry, -1)

    if pid_ets do
      {partition_pid, _} = pid_ets
      GenServer.call(partition_pid, :sync)
    else
      for i <- 0..(partitions - 1) do
        [{^i, _key_ets, {partition_pid, _pid_ets}}] = :ets.lookup(registry, i)
        GenServer.call(partition_pid, :sync)
      end
    end
  end

  def start_registry(name, keys, partitions \\ 1) do
    if pid = Process.whereis(name) do
      GenServer.stop(pid)
      Process.sleep(10)
    end

    Registry.start_link(keys: keys, name: name, partitions: partitions)
  end

  def run_bench(partitions) do
    p_label = "#{partitions}p"

    for n <- [100, 1_000, 10_000, 1_000_000] do
      IO.puts("--- N = #{n} ---\n")

      {:ok, _} = start_registry(:BenchDup, :duplicate, partitions)
      {:ok, _} = start_registry(:BenchOrd, {:duplicate, :ordered}, partitions)

      dup_workers = populate(:BenchDup, "hot_key", n)
      ord_workers = populate(:BenchOrd, "hot_key", n)

      benchee_opts = [warmup: 1, time: 3, print: [configuration: false]]

      # Register
      Benchee.run(
        %{
          "duplicate" => {
            fn pid ->
              register_worker(pid, :BenchDup, "hot_key")
              pid
            end,
            before_each: fn _ -> spawn_worker() end,
            after_each: fn pid ->
              unregister_worker(pid, :BenchDup, "hot_key")
              stop_worker(pid)
            end
          },
          "ordered" => {
            fn pid ->
              register_worker(pid, :BenchOrd, "hot_key")
              pid
            end,
            before_each: fn _ -> spawn_worker() end,
            after_each: fn pid ->
              unregister_worker(pid, :BenchOrd, "hot_key")
              stop_worker(pid)
            end
          }
        },
        [{:title, "Register N=#{n} #{p_label}"} | benchee_opts]
      )

      # Lookup
      Benchee.run(
        %{
          "duplicate" => fn -> Registry.lookup(:BenchDup, "hot_key") end,
          "ordered" => fn -> Registry.lookup(:BenchOrd, "hot_key") end
        },
        [{:title, "Lookup N=#{n} #{p_label}"} | benchee_opts]
      )

      # Unregister
      Benchee.run(
        %{
          "duplicate" => {
            fn pid ->
              unregister_worker(pid, :BenchDup, "hot_key")
              pid
            end,
            before_each: fn _ ->
              pid = spawn_worker()
              register_worker(pid, :BenchDup, "hot_key")
              pid
            end,
            after_each: fn pid -> stop_worker(pid) end
          },
          "ordered" => {
            fn pid ->
              unregister_worker(pid, :BenchOrd, "hot_key")
              pid
            end,
            before_each: fn _ ->
              pid = spawn_worker()
              register_worker(pid, :BenchOrd, "hot_key")
              pid
            end,
            after_each: fn pid -> stop_worker(pid) end
          }
        },
        [{:title, "Unregister N=#{n} #{p_label}"} | benchee_opts]
      )

      # Process death
      Benchee.run(
        %{
          "duplicate" => {
            fn pid ->
              mref = Process.monitor(pid)
              Process.exit(pid, :kill)
              receive do: ({:DOWN, ^mref, _, _, _} -> :ok)
              sync_registry(:BenchDup)
            end,
            before_each: fn _ ->
              pid = spawn_worker()
              register_worker(pid, :BenchDup, "hot_key")
              pid
            end
          },
          "ordered" => {
            fn pid ->
              mref = Process.monitor(pid)
              Process.exit(pid, :kill)
              receive do: ({:DOWN, ^mref, _, _, _} -> :ok)
              sync_registry(:BenchOrd)
            end,
            before_each: fn _ ->
              pid = spawn_worker()
              register_worker(pid, :BenchOrd, "hot_key")
              pid
            end
          }
        },
        [{:title, "Process death N=#{n} #{p_label}"} | benchee_opts]
      )

      Enum.each(dup_workers, &stop_worker/1)
      Enum.each(ord_workers, &stop_worker/1)
      GenServer.stop(Process.whereis(:BenchDup))
      GenServer.stop(Process.whereis(:BenchOrd))
    end
  end
end

IO.puts("\n=== 1 partition ===\n")
BenchHelper.run_bench(1)

partitions = System.schedulers_online()
IO.puts("\n=== #{partitions} partitions ===\n")
BenchHelper.run_bench(partitions)

IO.puts("\nDone.")
