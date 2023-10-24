use std::io;

mod bigquery_to_cloud_storage;
use bigquery_to_cloud_storage::btcs;

mod cloud_storage_clean_folder;
use cloud_storage_clean_folder::cscf;

mod cloud_storage_to_cloud_storage;
use cloud_storage_to_cloud_storage::cstcs;

mod cloud_storage_to_local_storage;
use cloud_storage_to_local_storage::cstls;

mod local_storage_to_cloud_storage;
use local_storage_to_cloud_storage::lstcs;


fn main() {

    let menu = r#"
        etl {command}

        Command list:
        - cloud_storage_clean_folder bucket_name file_name
        - 

    "#;

    println!("{}", menu);

    let mut input_command = String::new();

    match io::stdin().read_line(&mut input_command) {
        Ok(_) => println!(""),
        Err(err) => println!("Could not parse input: {}", err)
    }

    let kwargs = input_command.split(" ").collect::<Vec<_>>();
    let command = kwargs[0].trim();

    println!("{:?}", command);

    match command.trim() {
        "bigquery_to_cloud_storage" => match btcs::insert_data() {
            Ok(()) => println!("OKAY"),
            Err(err) => println!("{}", err),
        },
        "cloud_storage_clean_folder" => match cscf::delete_storage_object( 
            kwargs[1].trim(), kwargs[2].trim(),
            // "bucket_name", "file_name" 
        ) {
            Ok(()) => println!("Cloud storage location cleaned."),
            Err(err) => println!("{}", err),
        },
        "cloud_storage_to_bigquery" => println!("cloud_storage_to_bigquery selected"),
        "cloud_storage_to_local_storage" => match cstls::get_storage_objects( 
            kwargs[1].trim(), kwargs[2].trim(),
            // "bucket_name", "filename" 
        ) {
            Ok(()) => println!("File copied from cloud storage to local storage."),
            Err(err) => println!("{}", err),
        },
        "cloud_storage_to_cloud_storage" => match cstcs::copy_cloud_storage_to_cloud_storage(
            kwargs[1].trim(), kwargs[2].trim(), kwargs[3].trim(), kwargs[4].trim(),
           // "source_bucket", "source_file", "target_bucket", "target_file"
        ) {
            Ok(()) => println!("File copied from cloud storage to cloud storage."),
            Err(err) => println!("{}", err),
        },
        "local_storage_to_cloud_storage" => match lstcs::upload_file_to_gcs( "isaacs-test-bucket", "readme.md" ) {
            Ok(()) => println!("File copied from local storage to cloud storage."),
            Err(err) => println!("{}", err),
        },
        _ => println!("No valid option chosen.")
    }

}
