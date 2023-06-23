defmodule Main do
  def check(e) do
    if e != nil, do: raise e
  end

  def main do
    url = "https://randomuser.me/api/"
    file_path = "data.json"

    case HTTPoison.get(url) do
      {:ok, %{body: body}} ->
        case File.write(file_path, body) do
          :ok -> IO.puts("File saved successfully.")
          {:error, reason} -> IO.puts("Error writing file: #{reason}")
        end
      {:error, reason} ->
        IO.puts("Error requesting URL: #{reason}")
    end
  end
end

Main.main()
