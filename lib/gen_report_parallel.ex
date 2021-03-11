defmodule GenReportParallel do
  alias GenReportParallel.Parser

  def call do
    ["reports/part_1.csv", "reports/part_2.csv", "reports/part_3.csv"]
    |> Task.async_stream(&build/1)
    |> Enum.reduce(nil, fn {:ok, report}, acc ->
      sum_reports(report, acc)
    end)
  end

  defp build(file) do
    file
    |> File.stream!()
    |> Parser.parse_csv()
    |> gen_report()
  end

  defp sum_reports(report1, nil) do
    report1
  end

  # @TODO: Refatorar
  defp sum_reports(report1, report2) do
    Map.merge(report1, report2, fn key, val1, val2 ->
      case key do
        :all_hours ->
          Map.merge(val1, val2, fn _key, val1, val2 ->
            val1 + val2
          end)

        _ ->
          Map.merge(val1, val2, fn _key, val1, val2 ->
            Map.merge(val1, val2, fn _key, val1, val2 ->
              val1 + val2
            end)
          end)
      end
    end)
  end

  defp gen_report(data) do
    all_hours_task = Task.async(fn -> gen_all_hours(data) end)
    hours_per_month_task = Task.async(fn -> gen_hours_per_month(data) end)
    hours_per_year_task = Task.async(fn -> hours_per_year(data) end)

    %{
      all_hours: Task.await(all_hours_task),
      hours_per_month: Task.await(hours_per_month_task),
      hours_per_year: Task.await(hours_per_year_task)
    }
  end

  defp gen_all_hours(data) do
    data
    |> Enum.reduce(%{}, fn %{name: name, working_hours: hours}, acc ->
      if acc[name] == nil,
        do: Map.put(acc, name, hours),
        else: Map.put(acc, name, acc[name] + hours)
    end)
  end

  # @TODO: Refatorar
  defp gen_hours_per_month(data) do
    data
    |> Enum.reduce(%{}, fn %{name: name, working_hours: hours, month: month}, acc ->
      if acc[name] == nil do
        Map.put(acc, name, %{month => hours})
      else
        if acc[name][month] == nil do
          Map.put(acc, name, Map.put(acc[name], month, hours))
        else
          Map.put(acc, name, Map.put(acc[name], month, acc[name][month] + hours))
        end
      end
    end)
  end

  # @TODO: Refatorar
  defp hours_per_year(data) do
    data
    |> Enum.reduce(%{}, fn %{name: name, working_hours: hours, year: year}, acc ->
      if acc[name] == nil do
        Map.put(acc, name, %{year => hours})
      else
        if acc[name][year] == nil do
          Map.put(acc, name, Map.put(acc[name], year, hours))
        else
          Map.put(acc, name, Map.put(acc[name], year, acc[name][year] + hours))
        end
      end
    end)
  end
end
