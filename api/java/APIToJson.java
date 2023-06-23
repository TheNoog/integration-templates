import java.io.FileOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

public class APIToJson {
    public static void main(String[] args) {
        String url = "https://randomuser.me/api/";
        String filePath = "data.json";

        try {
            URL apiUrl = new URL(url);
            HttpURLConnection connection = (HttpURLConnection) apiUrl.openConnection();

            if (connection.getResponseCode() == HttpURLConnection.HTTP_OK) {
                byte[] responseBody = connection.getInputStream().readAllBytes();
                Files.write(Path.of(filePath), responseBody);
                System.out.println("File saved successfully.");
            } else {
                System.out.println("Error requesting URL: " + connection.getResponseCode());
            }

            connection.disconnect();
        } catch (IOException e) {
            System.out.println("An error occurred: " + e.getMessage());
        }
    }
}
