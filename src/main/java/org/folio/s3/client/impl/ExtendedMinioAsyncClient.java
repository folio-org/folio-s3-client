package org.folio.s3.client.impl;

import com.google.common.collect.Multimap;
import io.minio.AbortMultipartUploadResponse;
import io.minio.CreateMultipartUploadResponse;
import io.minio.MinioAsyncClient;
import io.minio.ObjectWriteResponse;
import io.minio.UploadPartCopyResponse;
import io.minio.UploadPartResponse;
import io.minio.errors.InsufficientDataException;
import io.minio.errors.InternalException;
import io.minio.errors.XmlParserException;
import io.minio.messages.Part;
import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.CompletableFuture;

/**
 * {@link MinioAsyncClient} with visibility of some methods raised
 * from protected to public.
 */
public class ExtendedMinioAsyncClient extends MinioAsyncClient {

  public ExtendedMinioAsyncClient(MinioAsyncClient client) {
    super(client);
  }

  public static ExtendedMinioAsyncClient build(Builder builder) {
    return new ExtendedMinioAsyncClient(builder.build());
  }

  @Override
  public CompletableFuture<CreateMultipartUploadResponse> createMultipartUploadAsync(
      String bucketName,
      String region,
      String objectName,
      Multimap<String, String> headers,
      Multimap<String, String> extraQueryParams)
          throws InsufficientDataException, InternalException, InvalidKeyException, IOException,
          NoSuchAlgorithmException, XmlParserException {

    return super.createMultipartUploadAsync(bucketName, region, objectName, headers, extraQueryParams);
  }

  @Override
  public CompletableFuture<UploadPartCopyResponse> uploadPartCopyAsync(
      String bucketName,
      String region,
      String objectName,
      String uploadId,
      int partNumber,
      Multimap<String, String> headers,
      Multimap<String, String> extraQueryParams)
          throws InsufficientDataException, InternalException, InvalidKeyException, IOException,
          NoSuchAlgorithmException, XmlParserException {

    return super.uploadPartCopyAsync(bucketName, region, objectName, uploadId, partNumber, headers, extraQueryParams);
  }

  @Override
  public CompletableFuture<UploadPartResponse> uploadPartAsync(
      String bucketName,
      String region,
      String objectName,
      Object data,
      long length,
      String uploadId,
      int partNumber,
      Multimap<String, String> extraHeaders,
      Multimap<String, String> extraQueryParams)
          throws InsufficientDataException, InternalException, InvalidKeyException, IOException,
          NoSuchAlgorithmException, XmlParserException {

    return super.uploadPartAsync(bucketName, region, objectName, data, length, uploadId, partNumber, extraHeaders, extraQueryParams);
  }

  @Override
  public CompletableFuture<ObjectWriteResponse> completeMultipartUploadAsync(
      String bucketName,
      String region,
      String objectName,
      String uploadId,
      Part[] parts,
      Multimap<String, String> extraHeaders,
      Multimap<String, String> extraQueryParams)
          throws InsufficientDataException, InternalException, InvalidKeyException, IOException,
          NoSuchAlgorithmException, XmlParserException {

    return super.completeMultipartUploadAsync(bucketName, region, objectName, uploadId, parts, extraHeaders, extraQueryParams);
  }

  @Override
  public CompletableFuture<AbortMultipartUploadResponse> abortMultipartUploadAsync(
      String bucketName,
      String region,
      String objectName,
      String uploadId,
      Multimap<String, String> extraHeaders,
      Multimap<String, String> extraQueryParams)
          throws InsufficientDataException, InternalException, InvalidKeyException, IOException,
          NoSuchAlgorithmException, XmlParserException {

    return super.abortMultipartUploadAsync(bucketName, region, objectName, uploadId, extraHeaders, extraQueryParams);
  }
}
