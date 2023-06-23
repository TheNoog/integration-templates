#include <stdio.h>
#include <stdlib.h>
#include <curl/curl.h>

// Callback function to write the response body into a file
size_t write_data(void *ptr, size_t size, size_t nmemb, FILE *stream) {
    return fwrite(ptr, size, nmemb, stream);
}

int main() {
    CURL *curl;
    FILE *file;
    CURLcode res;

    // Initialize the CURL session
    curl_global_init(CURL_GLOBAL_DEFAULT);
    curl = curl_easy_init();

    if (curl) {
        // Set the URL
        curl_easy_setopt(curl, CURLOPT_URL, "https://randomuser.me/api/");

        // Open the file for writing
        file = fopen("data.json", "wb");
        if (!file) {
            fprintf(stderr, "Error opening the file.\n");
            return 1;
        }

        // Set the write callback function
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_data);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, file);

        // Perform the request and store the response
        res = curl_easy_perform(curl);
        if (res != CURLE_OK) {
            fprintf(stderr, "curl_easy_perform() failed: %s\n", curl_easy_strerror(res));
            return 1;
        }

        // Clean up
        curl_easy_cleanup(curl);
        fclose(file);
    }

    // Clean up global CURL resources
    curl_global_cleanup();

    return 0;
}
