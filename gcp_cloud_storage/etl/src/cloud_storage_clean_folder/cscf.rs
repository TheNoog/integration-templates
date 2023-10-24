#![allow(dead_code)]

#[tokio::main]
pub async fn delete_storage_object(
    bucket: &str,
    object: &str,
) -> Result<(), Box<dyn std::error::Error>> {

    let subscriber = tracing_subscriber::fmt()
        .with_env_filter("gcloud_sdk=debug")
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    let google_rest_client = gcloud_sdk::GoogleRestApi::new().await?;

    // https://github.com/abdolence/gcloud-sdk-rs/blob/master/gcloud-sdk/src/rest_apis/google_rest_apis/storage_v1/apis/objects_api.rs
    let response = gcloud_sdk::google_rest_apis::storage_v1::objects_api::storage_objects_delete(
        &google_rest_client.create_google_storage_v1_config().await?,
        gcloud_sdk::google_rest_apis::storage_v1::objects_api::StoragePeriodObjectsPeriodDeleteParams {
            bucket: bucket.to_string(),
            object: object.to_string(),
            ..Default::default()
        }
    ).await?;

    println!("{:?}", response);

    Ok(())
}