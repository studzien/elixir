# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: 2021 The Elixir Team
# SPDX-FileCopyrightText: 2012 Plataformatec

Code.require_file("../test_helper.exs", __DIR__)

defmodule Registry.DuplicateOrderedTest do
  use ExUnit.Case,
    async: true,
    parameterize:
      for(
        partitions <- [1, 8],
        do: %{partitions: partitions}
      )

  setup config do
    partitions = config.partitions
    name = :"#{config.test}_#{partitions}"
    opts = [keys: {:duplicate, :ordered}, name: name, partitions: partitions]
    {:ok, _} = start_supervised({Registry, opts})
    %{registry: name}
  end

  describe "startup and table creation" do
    test "start_link with {:duplicate, :ordered} succeeds", %{registry: registry} do
      assert Process.whereis(registry) |> is_pid()
    end

    test "key_ets table type is :ordered_set", %{registry: registry, partitions: partitions} do
      if partitions == 1 do
        {_kind, _partitions, key_ets} = :ets.lookup_element(registry, -2, 2)
        assert :ets.info(key_ets, :type) == :ordered_set
      else
        for i <- 0..(partitions - 1) do
          [{^i, key_ets, _pid_ets}] = :ets.lookup(registry, i)
          assert :ets.info(key_ets, :type) == :ordered_set
        end
      end
    end

    test "pid_ets table type is :duplicate_bag", %{registry: registry, partitions: partitions} do
      if partitions == 1 do
        {_kind, _partitions, _key_ets, {_pid_server, pid_ets}, _listeners} =
          :ets.lookup_element(registry, -1, 2)

        assert :ets.info(pid_ets, :type) == :duplicate_bag
      else
        for i <- 0..(partitions - 1) do
          [{^i, _key_ets, {_pid_server, pid_ets}}] = :ets.lookup(registry, i)
          assert :ets.info(pid_ets, :type) == :duplicate_bag
        end
      end
    end

    test "multi-partition creates correct number of children",
         %{registry: registry, partitions: partitions} do
      assert length(Supervisor.which_children(registry)) == partitions
    end
  end

  test "invalid keys like {:duplicate, :invalid} are still rejected" do
    assert_raise ArgumentError, ~r/expected :keys to be given and be one of/, fn ->
      Registry.start_link(keys: {:duplicate, :invalid}, name: :test_invalid_ordered)
    end
  end

  describe "register and lookup" do
    test "register returns {:ok, pid}", %{registry: registry} do
      assert {:ok, pid} = Registry.register(registry, "hello", :value)
      assert is_pid(pid)
    end

    test "lookup returns [{pid, value}] after register", %{registry: registry} do
      {:ok, _} = Registry.register(registry, "hello", :world)
      assert Registry.lookup(registry, "hello") == [{self(), :world}]
    end

    test "lookup on empty key returns []", %{registry: registry} do
      assert Registry.lookup(registry, "hello") == []
    end

    test "multiple registrations under same key from same process",
         %{registry: registry} do
      {:ok, _} = Registry.register(registry, "hello", :value1)
      {:ok, _} = Registry.register(registry, "hello", :value2)

      result = Registry.lookup(registry, "hello") |> Enum.sort()
      assert result == [{self(), :value1}, {self(), :value2}]
    end

    test "registrations across processes", %{registry: registry} do
      parent = self()

      {:ok, task} =
        Task.start(fn ->
          send(parent, Registry.register(registry, "hello", :from_task))
          Process.sleep(:infinity)
        end)

      assert_receive {:ok, _owner}

      {:ok, _} = Registry.register(registry, "hello", :from_parent)

      result = Registry.lookup(registry, "hello") |> Enum.sort()
      assert length(result) == 2
      assert {self(), :from_parent} in result
      assert {task, :from_task} in result
    end

    test "values returns correct values filtered by pid", %{registry: registry} do
      {:ok, _} = Registry.register(registry, "hello", :value1)
      {:ok, _} = Registry.register(registry, "hello", :value2)

      assert Registry.values(registry, "hello", self()) |> Enum.sort() == [:value1, :value2]
    end

    test "values returns [] when pid has no registrations", %{registry: registry} do
      assert Registry.values(registry, "hello", self()) == []
    end

    test "reserved atom key register + lookup works", %{registry: registry} do
      {:ok, _} = Registry.register(registry, :_, :value)
      assert Registry.lookup(registry, :_) == [{self(), :value}]

      {:ok, _} = Registry.register(registry, :"$1", :value2)
      assert Registry.lookup(registry, :"$1") == [{self(), :value2}]
    end

    test "via tuple raises for ordered duplicate registry", %{registry: registry} do
      assert_raise ArgumentError, ":via is not supported for duplicate registries", fn ->
        name = {:via, Registry, {registry, "hello"}}
        Agent.start_link(fn -> 0 end, name: name)
      end
    end

    test "update_value is not supported", %{registry: registry} do
      assert_raise ArgumentError, ~r/Registry.update_value\/3 is not supported/, fn ->
        Registry.update_value(registry, "hello", fn val -> val end)
      end
    end
  end

  describe "dispatch" do
    test "dispatch invokes callback with correct {pid, value} entries",
         %{registry: registry} do
      {:ok, _} = Registry.register(registry, "hello", :value1)
      {:ok, _} = Registry.register(registry, "hello", :value2)

      parent = self()

      Registry.dispatch(registry, "hello", fn entries ->
        send(parent, {:entries, Enum.sort(entries)})
      end)

      assert_receive {:entries, [{pid, :value1}, {pid, :value2}]} when pid == parent
    end

    test "dispatch on nonexistent key does not invoke callback",
         %{registry: registry} do
      fun = fn _ -> raise "should not be invoked" end
      assert Registry.dispatch(registry, "nonexistent", fun) == :ok
    end

    test "dispatch with parallel: true works", %{registry: registry} do
      {:ok, _} = Registry.register(registry, "hello", :value1)
      {:ok, _} = Registry.register(registry, "hello", :value2)

      fun = fn entries ->
        for {pid, value} <- entries, do: send(pid, {:dispatch, value})
      end

      assert Registry.dispatch(registry, "hello", fun, parallel: true) == :ok

      assert_received {:dispatch, :value1}
      assert_received {:dispatch, :value2}
    end
  end

  describe "unregister" do
    test "unregister removes all entries for key from calling process",
         %{registry: registry} do
      {:ok, _} = Registry.register(registry, "hello", :value1)
      {:ok, _} = Registry.register(registry, "hello", :value2)
      {:ok, _} = Registry.register(registry, "world", :value3)

      :ok = Registry.unregister(registry, "hello")
      assert Registry.lookup(registry, "hello") == []
      assert Registry.lookup(registry, "world") == [{self(), :value3}]
    end

    test "unregister with reserved atom key", %{registry: registry} do
      {:ok, _} = Registry.register(registry, :_, :foo)
      {:ok, _} = Registry.register(registry, :_, :bar)
      {:ok, _} = Registry.register(registry, "hello", :value)

      :ok = Registry.unregister(registry, :_)
      assert Registry.lookup(registry, :_) == []
      assert Registry.lookup(registry, "hello") == [{self(), :value}]
    end

    test "unregister with no entries is no-op", %{registry: registry} do
      assert Registry.unregister(registry, "hello") == :ok
    end

    test "unregister unlinks when no more entries", %{registry: registry} do
      {:ok, pid} = Registry.register(registry, "hello", :value)
      {:links, links} = Process.info(self(), :links)
      assert pid in links

      :ok = Registry.unregister(registry, "hello")
      {:links, links} = Process.info(self(), :links)
      refute pid in links
    end

    test "other processes entries unaffected by unregister", %{registry: registry} do
      parent = self()

      {:ok, task} =
        Task.start(fn ->
          send(parent, Registry.register(registry, "hello", :from_task))
          Process.sleep(:infinity)
        end)

      assert_receive {:ok, _owner}

      {:ok, _} = Registry.register(registry, "hello", :from_parent)
      :ok = Registry.unregister(registry, "hello")

      assert Registry.lookup(registry, "hello") == [{task, :from_task}]
    end
  end

  describe "match and count_match" do
    test "match with simple pattern returns correct pairs", %{registry: registry} do
      value1 = {1, :atom, 1}
      value2 = {2, :atom, 2}

      {:ok, _} = Registry.register(registry, "hello", value1)
      {:ok, _} = Registry.register(registry, "hello", value2)

      assert Registry.match(registry, "hello", {1, :_, :_}) == [{self(), value1}]
      assert Registry.match(registry, "hello", {2, :_, :_}) == [{self(), value2}]

      assert Registry.match(registry, "hello", {:_, :atom, :_}) |> Enum.sort() ==
               [{self(), value1}, {self(), value2}]
    end

    test "match with capture variables works", %{registry: registry} do
      value1 = {1, :atom, 1}
      value2 = {2, :atom, 2}

      {:ok, _} = Registry.register(registry, "hello", value1)
      {:ok, _} = Registry.register(registry, "hello", value2)

      assert Registry.match(registry, "hello", {:"$1", :_, :"$1"}) |> Enum.sort() ==
               [{self(), value1}, {self(), value2}]
    end

    test "match with guards works", %{registry: registry} do
      value1 = {1, :atom, 1}
      value2 = {2, :atom, 2}

      {:ok, _} = Registry.register(registry, "hello", value1)
      {:ok, _} = Registry.register(registry, "hello", value2)

      assert Registry.match(registry, "hello", {:"$1", :_, :_}, [{:<, :"$1", 2}]) ==
               [{self(), value1}]
    end

    test "match with reserved atom key works", %{registry: registry} do
      {:ok, _} = Registry.register(registry, :_, :value)
      assert Registry.match(registry, :_, :_) == [{self(), :value}]
    end

    test "match on nonexistent key returns []", %{registry: registry} do
      assert Registry.match(registry, "nonexistent", :_) == []
    end

    test "count_match returns correct count", %{registry: registry} do
      {:ok, _} = Registry.register(registry, "hello", {1, :atom, 1})
      {:ok, _} = Registry.register(registry, "hello", {2, :atom, 2})

      assert Registry.count_match(registry, "hello", {1, :_, :_}) == 1
      assert Registry.count_match(registry, "hello", {:_, :atom, :_}) == 2
      assert Registry.count_match(registry, "hello", :_) == 2
      assert Registry.count_match(registry, :_, :_) == 0
    end

    test "count_match with guards works", %{registry: registry} do
      {:ok, _} = Registry.register(registry, "hello", {1, :atom, 2})

      assert Registry.count_match(registry, "hello", {:_, :_, :"$1"}, [{:>, :"$1", 1}]) == 1
      assert Registry.count_match(registry, "hello", {:_, :_, :"$1"}, [{:>, :"$1", 2}]) == 0
    end
  end

  describe "select and count_select" do
    test "select returns [{key, pid, value}] triples", %{registry: registry} do
      {:ok, _} = Registry.register(registry, "hello", :value)
      {:ok, _} = Registry.register(registry, "hello", :value)

      result =
        Registry.select(registry, [{{:"$1", :"$2", :"$3"}, [], [{{:"$1", :"$2", :"$3"}}]}])
        |> Enum.sort()

      assert result == [{"hello", self(), :value}, {"hello", self(), :value}]
    end

    test "select with bound key works", %{registry: registry} do
      value = {1, :atom, 1}
      {:ok, _} = Registry.register(registry, "hello", value)

      assert [{"hello", self(), value}] ==
               Registry.select(registry, [
                 {{"hello", :"$2", :"$3"}, [], [{{"hello", :"$2", :"$3"}}]}
               ])
    end

    test "select with guards works", %{registry: registry} do
      value = {1, :atom, 2}
      {:ok, _} = Registry.register(registry, "hello", value)

      assert [{"hello", self(), {1, :atom, 2}}] ==
               Registry.select(registry, [
                 {{:"$1", :"$2", {:"$3", :"$4", :"$5"}}, [{:>, :"$5", 1}],
                  [{{:"$1", :"$2", {{:"$3", :"$4", :"$5"}}}}]}
               ])
    end

    test "select with multiple specs works", %{registry: registry} do
      {:ok, _} = Registry.register(registry, "hello", :value)
      {:ok, _} = Registry.register(registry, "world", :value)

      result =
        Registry.select(registry, [
          {{"hello", :_, :_}, [], [{:element, 1, :"$_"}]},
          {{"world", :_, :_}, [], [{:element, 1, :"$_"}]}
        ])
        |> Enum.sort()

      assert result == ["hello", "world"]
    end

    test "count_select returns correct count", %{registry: registry} do
      {:ok, _} = Registry.register(registry, "hello", :value)
      {:ok, _} = Registry.register(registry, "world", :value)

      assert 2 == Registry.count_select(registry, [{{:_, :_, :_}, [], [true]}])
      assert 1 == Registry.count_select(registry, [{{"hello", :_, :_}, [], [true]}])
    end

    test "empty registry returns empty list", %{registry: registry} do
      assert Registry.select(registry, [{{:_, :_, :_}, [], [:"$_"]}]) == []
    end

    test "select raises on invalid spec shape", %{registry: registry} do
      assert_raise ArgumentError, ~r/invalid match specification/, fn ->
        Registry.select(registry, [{:_, [], [:"$_"]}])
      end
    end
  end

  describe "unregister_match" do
    test "unregister_match with pattern that matches all entries",
         %{registry: registry} do
      {:ok, _} = Registry.register(registry, "hello", {1, :atom, 1})
      {:ok, _} = Registry.register(registry, "hello", {2, :atom, 2})

      Registry.unregister_match(registry, "hello", {:_, :atom, :_})
      assert Registry.lookup(registry, "hello") == []
    end

    test "unregister_match with pattern that matches no entries",
         %{registry: registry} do
      {:ok, _} = Registry.register(registry, "hello", {1, :atom, 1})

      Registry.unregister_match(registry, "hello", {2, :_, :_})
      assert Registry.lookup(registry, "hello") == [{self(), {1, :atom, 1}}]
    end

    test "unregister_match with pattern that matches some entries (partial delete)",
         %{registry: registry} do
      value1 = {1, :atom, 1}
      value2 = {2, :atom, 2}

      {:ok, _} = Registry.register(registry, "hello", value1)
      {:ok, _} = Registry.register(registry, "hello", value2)

      Registry.unregister_match(registry, "hello", {2, :_, :_})
      assert Registry.lookup(registry, "hello") == [{self(), value1}]
    end

    test "unregister_match with guards", %{registry: registry} do
      value1 = {1, :atom, 1}
      value2 = {2, :atom, 2}

      {:ok, _} = Registry.register(registry, "hello", value1)
      {:ok, _} = Registry.register(registry, "hello", value2)

      Registry.unregister_match(registry, "hello", {:"$1", :_, :_}, [{:<, :"$1", 2}])
      assert Registry.lookup(registry, "hello") == [{self(), value2}]
    end

    test "unregister_match with reserved atom key", %{registry: registry} do
      {:ok, _} = Registry.register(registry, :_, :foo)
      {:ok, _} = Registry.register(registry, :_, :bar)
      {:ok, _} = Registry.register(registry, "hello", "a")

      Registry.unregister_match(registry, :_, :foo)
      assert Registry.lookup(registry, :_) == [{self(), :bar}]
      assert Registry.keys(registry, self()) |> Enum.sort() == [:_, "hello"]
    end
  end

  describe "process death cleanup" do
    test "process crash removes all entries from ordered_set key_ets",
         %{registry: registry, partitions: partitions} do
      {_, task1} = register_task(registry, "hello", :value)
      {_, task2} = register_task(registry, "world", :value)

      kill_and_assert_down(task1)
      kill_and_assert_down(task2)

      if partitions > 1 do
        for i <- 0..(partitions - 1) do
          [{_, _, {partition, _}}] = :ets.lookup(registry, i)
          GenServer.call(partition, :sync)
        end

        for i <- 0..(partitions - 1) do
          [{_, key, {_, pid}}] = :ets.lookup(registry, i)
          assert :ets.tab2list(key) == []
          assert :ets.tab2list(pid) == []
        end
      else
        [{-1, {_, _, key, {partition, pid}, _}}] = :ets.lookup(registry, -1)
        GenServer.call(partition, :sync)
        assert :ets.tab2list(key) == []
        assert :ets.tab2list(pid) == []
      end
    end

    test "process crash with entries under multiple keys cleans all",
         %{registry: registry, partitions: partitions} do
      parent = self()

      {:ok, task} =
        Task.start(fn ->
          send(parent, Registry.register(registry, "hello", :v1))
          send(parent, Registry.register(registry, "world", :v2))
          Process.sleep(:infinity)
        end)

      assert_receive {:ok, _}
      assert_receive {:ok, _}

      kill_and_assert_down(task)

      # Sync all partitions
      if partitions > 1 do
        for i <- 0..(partitions - 1) do
          [{_, _, {partition, _}}] = :ets.lookup(registry, i)
          GenServer.call(partition, :sync)
        end
      else
        [{-1, {_, _, _, {partition, _}, _}}] = :ets.lookup(registry, -1)
        GenServer.call(partition, :sync)
      end

      assert Registry.lookup(registry, "hello") == []
      assert Registry.lookup(registry, "world") == []
    end

    test "process crash with reserved atom keys cleans correctly",
         %{registry: registry, partitions: partitions} do
      parent = self()

      {:ok, task} =
        Task.start(fn ->
          send(parent, Registry.register(registry, :_, :value))
          Process.sleep(:infinity)
        end)

      assert_receive {:ok, _}

      kill_and_assert_down(task)

      # Sync all partitions
      if partitions > 1 do
        for i <- 0..(partitions - 1) do
          [{_, _, {partition, _}}] = :ets.lookup(registry, i)
          GenServer.call(partition, :sync)
        end
      else
        [{-1, {_, _, _, {partition, _}, _}}] = :ets.lookup(registry, -1)
        GenServer.call(partition, :sync)
      end

      assert Registry.lookup(registry, :_) == []
    end
  end

  defp register_task(registry, key, value) do
    parent = self()

    {:ok, task} =
      Task.start(fn ->
        send(parent, Registry.register(registry, key, value))
        Process.sleep(:infinity)
      end)

    assert_receive {:ok, owner}
    {owner, task}
  end

  defp kill_and_assert_down(pid) do
    ref = Process.monitor(pid)
    Process.exit(pid, :kill)
    assert_receive {:DOWN, ^ref, _, _, _}
  end
end
