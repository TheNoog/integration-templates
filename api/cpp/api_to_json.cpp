#include <iostream>
#include <fstream>
#include <curl/curl.h>

// Callback function to write the response body into a file
size_t write_data(void* ptr, size_t size, size_t nmemb, std::ofstream* stream) {
    stream->write(static_cast<char*>(ptr), size * nmemb);
    return size * nmemb;
}

int main() {
    CURL* curl;
    std::ofstream file;

    // Initialize the CURL session
    curl_global_init(CURL_GLOBAL_DEFAULT);
    curl = curl_easy_init();

    if (curl) {
        // Set the URL
        curl_easy_setopt(curl, CURLOPT_URL, "https://randomuser.me/api/");

        // Open the file for writing
        file.open("data.json", std::ios::binary);
        if (!file.is_open()) {
            std::cerr << "Error opening the file." << std::endl;
            return 1;
        }

        // Set the write callback function
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_data);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, &file);

        // Perform the request and store the response
        CURLcode res = curl_easy_perform(curl);
        if (res != CURLE_OK) {
            std::cerr << "curl_easy_perform() failed: " << curl_easy_strerror(res) << std::endl;
            return 1;
        }

        // Clean up
        curl_easy_cleanup(curl);
        file.close();
    }

    // Clean up global CURL resources
    curl_global_cleanup();

    return 0;
}
