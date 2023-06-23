require 'net/http'
require 'open-uri'

url = 'https://randomuser.me/api/'
file_path = 'data.json'

begin
  response = URI.open(url).read
  open(file_path, 'w') do |file|
    file.write(response)
  end
  puts 'File saved successfully.'
rescue StandardError => e
  puts "An error occurred: #{e.message}"
end
