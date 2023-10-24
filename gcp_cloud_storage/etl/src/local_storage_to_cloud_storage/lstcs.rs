#![allow(dead_code)]

// https://github.com/abdolence/gcloud-sdk-rs/blob/master/examples/gcs-rest-client/src/main.rs
#[tokio::main]
pub async fn upload_file_to_gcs(
    bucket: &str,
    filename: &str,
) -> Result<(), Box<dyn std::error::Error>> {

    let subscriber = tracing_subscriber::fmt()
        .with_env_filter("gcloud_sdk=debug")
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    let google_rest_client = gcloud_sdk::GoogleRestApi::new().await?;

    // get bytes from file
    let file = std::fs::read(&filename).unwrap();

    let response = gcloud_sdk::google_rest_apis::storage_v1::objects_api::storage_objects_insert_ext_bytes(
        &google_rest_client.create_google_storage_v1_config().await?,
        gcloud_sdk::google_rest_apis::storage_v1::objects_api::StoragePeriodObjectsPeriodInsertParams {
            bucket: bucket.to_string(),
            name: Some(filename.to_string()),
            ..Default::default()
        },
        None,
        file //.as_bytes().to_vec()
    ).await?;

    println!("{:?}", response);

    Ok(())
}