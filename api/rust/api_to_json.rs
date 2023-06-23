use std::fs::File;
use std::io::Write;
use std::io::Read;
use reqwest;

fn main() {
    let url = "https://randomuser.me/api/";
    let file_path = "data.json";

    match reqwest::blocking::get(url) {
        Ok(mut response) => {
            let mut body = Vec::new();
            response.read_to_end(&mut body).unwrap();

            match File::create(file_path) {
                Ok(mut file) => {
                    file.write_all(&body).unwrap();
                    println!("File saved successfully.");
                }
                Err(e) => {
                    eprintln!("Error creating file: {}", e);
                }
            }
        }
        Err(e) => {
            eprintln!("Error requesting URL: {}", e);
        }
    }
}
