use std::error::Error;
use std::fmt::Display;
use std::time::Duration;

use aws_config::Region;
use aws_sdk_s3::config::Credentials;
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::presigning::PresigningConfig;
use aws_sdk_s3::primitives::{ByteStream, ByteStreamError};
use aws_sdk_s3::Client;
use bytes::Bytes;
use futures_util::TryStreamExt;
use http_body::Frame;
use tokio::io::AsyncRead;
use tokio_stream::Stream;
use url::Url;
use uuid::Uuid;

pub type FileId = Uuid;

#[derive(Debug, Clone)]
pub struct BucketRepository {
    client: Client,
    bucket_name: String,
}

impl BucketRepository {
    pub async fn new(
        endpoint_url: &str,
        region: &str,
        bucket_name: &str,
        access_key_id: &str,
        secret_key: &str,
    ) -> BucketRepository {
        tracing::info!("Connecting to bucket {bucket_name}");
        let config = aws_sdk_s3::Config::builder()
            .endpoint_url(endpoint_url)
            .credentials_provider(Credentials::new(access_key_id, secret_key, None, None, ""))
            .behavior_version_latest()
            .region(Region::new(region.to_string()))
            .build();

        let client = Client::from_conf(config);

        match client.head_bucket().bucket(bucket_name).send().await {
            Ok(_) => tracing::info!("Successfully connected to bucket"),
            Err(_) => {
                tracing::info!("Bucket {bucket_name} not found. Attempting to create it.");
                client
                    .create_bucket()
                    .bucket(bucket_name)
                    .send()
                    .await
                    .expect("Failed to create bucket");
            }
        }

        BucketRepository {
            client,
            bucket_name: bucket_name.to_string(),
        }
    }
}

impl BucketRepository {
    pub async fn delete_file(&self, path: &str, file_id: FileId) -> Result<(), BucketError> {
        let object_key = format!("{}{}", path, file_id); // Concatenate path and file_id

        self.client
            .delete_object()
            .bucket(&self.bucket_name)
            .key(object_key)
            .send()
            .await?;
        Ok(())
    }

    /// Example usage from a poem body:
    /// ``` text
    ///  async fn upload_file(&self, Binary(body): Binary<poem::Body>) -> Result<Uuid>{
    ///     let file_id = self
    ///         .bucket_repository
    ///         .put_file_stream(body.into_bytes_stream())
    ///         .await
    ///         .map_err(internal_server_error)?;
    ///     Ok(file_id)
    /// }
    pub async fn put_file_stream(
        &self,
        path: &str,
        content_type: &str,
        stream: impl Stream<Item = Result<Bytes, std::io::Error>> + Send + Sync + 'static,
    ) -> Result<FileId, BucketError> {
        let file_id = Uuid::new_v4();
        let object_key = format!("{}{}", path, file_id); // Concatenate path and file_id
        let stream = stream.map_ok(Frame::data);
        let stream_body = http_body_util::StreamBody::new(stream);
        let bytes_stream = ByteStream::from_body_1_x(stream_body);
        self.client
            .put_object()
            .bucket(&self.bucket_name)
            .key(object_key)
            .content_type(content_type)
            .body(bytes_stream)
            .send()
            .await?;

        Ok(file_id)
    }
    /// Example usage to a poem body:
    /// ``` text
    /// async fn get_image(&self, Path(image_id): Path<Uuid>) -> Result<Binary<Body>> {
    ///     let image_stream = self
    ///         .bucket_repository
    ///         .get_file_stream(image_id)
    ///         .await
    ///         .map_err(internal_server_error)?;
    ///
    ///     let body = poem::Body::from_async_read(image_stream);
    ///     Ok(Binary(body))
    pub async fn get_file_stream(
        &self,
        path: &str,
        file_id: FileId,
    ) -> Result<impl AsyncRead, BucketError> {
        let object_key = format!("{}{}", path, file_id); // Concatenate path and file_id

        let stream = self
            .client
            .get_object()
            .bucket(&self.bucket_name)
            .key(object_key)
            .send()
            .await?
            .body
            .into_async_read();

        Ok(stream)
    }

    pub async fn get_file(&self, path: &str, file_id: FileId) -> Result<Bytes, BucketError> {
        let object_key = format!("{}{}", path, file_id); // Concatenate path and file_id

        let aggregated_bytes = self
            .client
            .get_object()
            .bucket(&self.bucket_name)
            .key(object_key)
            .send()
            .await?
            .body
            .collect()
            .await?;

        Ok(aggregated_bytes.into_bytes())
    }

    pub async fn put_file(
        &self,
        path: &str,
        content_type: &str,
        bytes: Bytes,
    ) -> Result<FileId, BucketError> {
        let file_id = Uuid::new_v4();
        let object_key = format!("{}{}", path, file_id); // Concatenate path and file_id
        let stream = ByteStream::from(bytes);
        self.client
            .put_object()
            .bucket(&self.bucket_name)
            .key(object_key)
            .content_type(content_type)
            .body(stream)
            .send()
            .await?;

        Ok(file_id)
    }

    pub async fn get_presigned_post_url(&self, path: &str) -> Result<(FileId, Url), BucketError> {
        let file_id = Uuid::new_v4();
        let object_key = format!("{}{}", path, file_id); // Concatenate path and file_id

        let request = self
            .client
            .put_object()
            .bucket(&self.bucket_name)
            .key(object_key)
            .presigned(
                PresigningConfig::expires_in(Duration::from_secs(60 * 60 * 24 * 6)).expect(
                    "This is infallible. Expiry is below 1 week, which is checked at runtime.",
                ),
            )
            .await?;

        Ok((file_id, request.uri().try_into()?))
    }

    pub async fn get_presigned_get_url(
        &self,
        path: &str,
        file_id: FileId,
    ) -> Result<Url, BucketError> {
        let object_key = format!("{}{}", path, file_id); // Concatenate path and file_id
        let request = self
            .client
            .get_object()
            .bucket(&self.bucket_name)
            .key(object_key)
            .presigned(
                PresigningConfig::expires_in(Duration::from_secs(60 * 60 * 24 * 6)).expect(
                    "This is infallible. Expiry is below 1 week, which is checked at runtime.",
                ),
            )
            .await?;

        Ok(request.uri().try_into()?)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum BucketError {
    S3SdkError(String),
    ByteStreamError(String),
    UrlError(url::ParseError),
}

impl Display for BucketError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BucketError::S3SdkError(error) => write!(f, "S3 SDK Error: {}", error),
            BucketError::ByteStreamError(error) => write!(f, "ByteStream Error: {}", error),
            BucketError::UrlError(error) => write!(f, "URL Error: {}", error),
        }
    }
}

impl<T: Error + Send + Sync + 'static> From<SdkError<T>> for BucketError {
    fn from(error: SdkError<T>) -> Self {
        BucketError::S3SdkError(error.into_source().unwrap().to_string())
    }
}

impl From<ByteStreamError> for BucketError {
    fn from(error: ByteStreamError) -> Self {
        BucketError::ByteStreamError(error.to_string())
    }
}

impl From<url::ParseError> for BucketError {
    fn from(error: url::ParseError) -> Self {
        BucketError::UrlError(error)
    }
}
