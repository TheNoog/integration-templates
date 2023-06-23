using System;
using System.IO;
using System.Net;

class Program
{
    static void Main()
    {
        string url = "https://randomuser.me/api/";
        string filePath = "data.json";

        try
        {
            using (var client = new WebClient())
            {
                byte[] response = client.DownloadData(url);
                File.WriteAllBytes(filePath, response);
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine("An error occurred: " + ex.Message);
        }
    }
}
