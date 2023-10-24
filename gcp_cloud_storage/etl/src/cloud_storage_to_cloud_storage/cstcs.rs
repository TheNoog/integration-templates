#![allow(dead_code)]

// https://github.com/abdolence/gcloud-sdk-rs/blob/master/gcloud-sdk/src/rest_apis/google_rest_apis/storage_v1/apis/objects_api.rs
// https://github.com/abdolence/gcloud-sdk-rs/blob/master/examples/gcs-rest-client/src/main.rs

// use gcloud_sdk::GoogleRestApi;

// Upload to GCS has a slightly different API that OpenAPI doesn't support, so there is an extension method in this library to support this
#[tokio::main]
pub async fn copy_cloud_storage_to_cloud_storage(
    source_bucket: &str,
    source_object: &str,
    destination_bucket: &str,
    destination_object: &str,
) -> Result<(), Box<dyn std::error::Error>> {

    // https://github.com/abdolence/gcloud-sdk-rs/blob/master/gcloud-sdk/src/rest_apis/google_rest_apis/storage_v1/apis/objects_api.rs
    // Line 764
    let google_rest_client = gcloud_sdk::GoogleRestApi::new().await?;
    let response = gcloud_sdk::google_rest_apis::storage_v1::objects_api::storage_objects_copy(
        &google_rest_client.create_google_storage_v1_config().await?,
        gcloud_sdk::google_rest_apis::storage_v1::objects_api::StoragePeriodObjectsPeriodCopyParams {
            source_bucket: source_bucket.to_string(),
            source_object: source_object.to_string(),
            destination_bucket: destination_bucket.to_string(),
            destination_object: destination_object.to_string(),
            ..Default::default()
        }
    ).await?;

    println!("{:?}", response);

    Ok(())
}