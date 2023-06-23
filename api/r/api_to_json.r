library(httr)

url <- "https://randomuser.me/api/"
file_path <- "data.json"

tryCatch({
  response <- GET(url)
  if (http_status(response)$status_code == 200) {
    body <- content(response, "text")
    writeLines(body, file_path)
    cat("File saved successfully.\n")
  } else {
    cat("Error requesting URL:", http_status(response)$status_code, "\n")
  }
}, error = function(e) {
  cat("An error occurred:", conditionMessage(e), "\n")
})
