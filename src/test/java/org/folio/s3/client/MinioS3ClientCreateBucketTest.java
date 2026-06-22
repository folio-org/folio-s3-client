package org.folio.s3.client;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.CompletableFuture;

import org.folio.s3.client.impl.ExtendedMinioAsyncClient;
import org.folio.s3.exception.S3ClientException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.minio.BucketExistsArgs;
import io.minio.MakeBucketArgs;

@ExtendWith(MockitoExtension.class)
class MinioS3ClientCreateBucketTest {

  private static final String BUCKET = "test-bucket";
  private static final String REGION = "us-east-1";

  @Mock
  private ExtendedMinioAsyncClient minioClient;

  private MinioS3Client buildClient(String bucket) {
    var props = S3ClientProperties.builder()
      .endpoint("http://localhost:9000")
      .region(REGION)
      .bucket(bucket)
      .accessKey("ak")
      .secretKey("sk")
      .build();
    return new MinioS3Client(props, minioClient);
  }

  @Test
  void createBucketIfNotExists_whenBucketNameIsBlank_doesNothing() throws Exception {
    var client = buildClient("   ");

    assertDoesNotThrow(client::createBucketIfNotExists);

    verify(minioClient, never()).bucketExists(any(BucketExistsArgs.class));
    verify(minioClient, never()).makeBucket(any(MakeBucketArgs.class));
  }

  @Test
  void createBucketIfNotExists_whenBucketAlreadyExists_doesNotCallMakeBucket() throws Exception {
    when(minioClient.bucketExists(any(BucketExistsArgs.class)))
      .thenReturn(CompletableFuture.completedFuture(true));

    var client = buildClient(BUCKET);
    assertDoesNotThrow(client::createBucketIfNotExists);

    verify(minioClient).bucketExists(any(BucketExistsArgs.class));
    verify(minioClient, never()).makeBucket(any(MakeBucketArgs.class));
  }

  @Test
  void createBucketIfNotExists_whenBucketDoesNotExist_createsBucket() throws Exception {
    when(minioClient.bucketExists(any(BucketExistsArgs.class)))
      .thenReturn(CompletableFuture.completedFuture(false));
    when(minioClient.makeBucket(any(MakeBucketArgs.class)))
      .thenReturn(CompletableFuture.completedFuture(null));

    var client = buildClient(BUCKET);
    assertDoesNotThrow(client::createBucketIfNotExists);

    verify(minioClient).bucketExists(any(BucketExistsArgs.class));
    verify(minioClient).makeBucket(any(MakeBucketArgs.class));
  }

  @Test
  void createBucketIfNotExists_whenBucketExistsCheckThrows_throwsS3ClientException() throws Exception {
    when(minioClient.bucketExists(any(BucketExistsArgs.class)))
      .thenReturn(CompletableFuture.failedFuture(new RuntimeException("connection refused")));

    var client = buildClient(BUCKET);

    assertThrows(S3ClientException.class, client::createBucketIfNotExists);
    verify(minioClient, never()).makeBucket(any(MakeBucketArgs.class));
  }
}
