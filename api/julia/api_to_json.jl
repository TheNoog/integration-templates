using HTTP, FileIO

function main()
    url = "https://randomuser.me/api/"
    file_path = "data.json"

    try
        response = HTTP.get(url)
        if response.status == 200
            body = String(response.body)
            FileIO.write(file_path, body)
            println("File saved successfully.")
        else
            println("Error requesting URL: $(response.status)")
        end
    catch e
        println("An error occurred: $(e)")
    end
end

main()
