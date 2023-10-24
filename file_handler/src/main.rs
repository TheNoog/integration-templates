mod csv_to_parquet;
mod parquet_to_csv;

use std::io;

use csv_to_parquet::ctp;
use parquet_to_csv::ptc;


fn main() {

    let menu = r#"
        file_handler {directory/filename} {instruction}

        Instructions:
            ctp = csv conversion to parquet
            ptc = parquet to csv
    "#;

    println!("{}", menu);

    let mut input_filename = String::new();

    match io::stdin().read_line(&mut input_filename) {
        Ok(_) => println!(""),
        Err(err) => println!("Could not parse input: {}", err)
    }

    let kwargs = input_filename.split(" ").collect::<Vec<_>>();
    let filename = kwargs[0].trim();
    let instruction = kwargs[1].trim();
    // println!("{:?}", filename);
    // println!("{:?}", instruction);

    match instruction.trim() {
        "ctp" => ctp::csv_to_parquet(filename),
        "ptc" => ptc::parquet_to_csv(),
        _ => println!("No valid option chosen.")
    }

}
