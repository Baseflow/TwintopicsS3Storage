use std::error::Error;
use std::time::Duration;

use aws_config::Region;
use aws_sdk_s3::Client;
use aws_sdk_s3::config::Credentials;
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::presigning::PresigningConfig;
use aws_sdk_s3::primitives::{ByteStream, ByteStreamError};
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
    pub fn new(
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
        tracing::info!("Successfully connected to bucket");

        BucketRepository {
            client,
            bucket_name: bucket_name.to_string(),
        }
    }
}

impl BucketRepository {
    pub async fn delete_file(&self, file_id: FileId) -> Result<(), BucketError> {
        self.client
            .delete_object()
            .bucket(&self.bucket_name)
            .key(&file_id.to_string())
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
        stream: impl Stream<Item=Result<Bytes, std::io::Error>> + Send + Sync + 'static,
    ) -> Result<FileId, BucketError> {
        let file_id = Uuid::new_v4();
        let stream = stream.map_ok(Frame::data);
        let stream_body = http_body_util::StreamBody::new(stream);
        let bytes_stream = ByteStream::from_body_1_x(stream_body);
        self.client
            .put_object()
            .bucket(&self.bucket_name)
            .key(&file_id.to_string())
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
    pub async fn get_file_stream(&self, file_id: FileId) -> Result<impl AsyncRead, BucketError> {
        let stream = self
            .client
            .get_object()
            .bucket(&self.bucket_name)
            .key(&file_id.to_string())
            .send()
            .await?
            .body
            .into_async_read();

        Ok(stream)
    }

    pub async fn get_file(&self, file_id: FileId) -> Result<Bytes, BucketError> {
        let aggregated_bytes = self
            .client
            .get_object()
            .bucket(&self.bucket_name)
            .key(&file_id.to_string())
            .send()
            .await?
            .body
            .collect()
            .await?;

        Ok(aggregated_bytes.into_bytes())
    }

    pub async fn put_file(&self, bytes: Bytes) -> Result<FileId, BucketError> {
        let file_id = Uuid::new_v4();
        let stream = ByteStream::from(bytes);
        self.client
            .put_object()
            .bucket(&self.bucket_name)
            .key(&file_id.to_string())
            .body(stream)
            .send()
            .await?;

        Ok(file_id)
    }

    pub async fn get_presigned_post_url(&self) -> Result<(FileId, Url), BucketError> {
        let file_id = Uuid::new_v4();
        // This library checks whether the pre-signing configuration is valid at runtime. (bad design!)
        // We know it is, because we are passing an expiry below 1 week. Therefore: unwrap
        let request = self
            .client
            .put_object()
            .bucket(&self.bucket_name)
            .key(&file_id.to_string())
            .presigned(PresigningConfig::expires_in(Duration::from_secs(60 * 60 * 24 * 6)).unwrap())
            .await?;

        Ok((file_id, request.uri().try_into()?))
    }

    pub async fn get_presigned_get_url(&self, file_id: FileId) -> Result<Url, BucketError> {
        // This library checks whether the pre-signing configuration is valid at runtime. (bad design!)
        // We know it is, because we are passing an expiry below 1 week. Therefore: unwrap
        let request = self
            .client
            .get_object()
            .bucket(&self.bucket_name)
            .key(&file_id.to_string())
            .presigned(PresigningConfig::expires_in(Duration::from_secs(60 * 60 * 24 * 6)).unwrap())
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
