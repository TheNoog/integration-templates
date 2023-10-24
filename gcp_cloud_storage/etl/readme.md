# ETL - Extract Transform Load

## Environment setup

https://crates.io/crates/gcloud-sdk/0.20.7
https://github.com/abdolence/gcloud-sdk-rs/blob/master/README.md
https://docs.rs/gcloud-sdk/latest/gcloud_sdk/

```
while read line; do export $line; done < .env

while read line; do export $line; done < developer/rust/etl/.env
```

For testing local:
gcloud auth login
gcloud auth application-default login

For deployment:
A JSON file whose path is specified by the GOOGLE_APPLICATION_CREDENTIALS environment variable.