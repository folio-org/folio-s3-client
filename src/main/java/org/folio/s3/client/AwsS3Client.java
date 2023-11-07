package org.folio.s3.client;

import static io.minio.ObjectWriteArgs.MIN_MULTIPART_SIZE;

import java.io.InputStream;
import java.io.SequenceInputStream;
import java.net.URI;

import org.apache.commons.lang3.StringUtils;
import org.folio.s3.exception.S3ClientException;

import lombok.extern.log4j.Log4j2;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.UploadPartCopyRequest;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;

@Log4j2
public class AwsS3Client extends MinioS3Client {

  private final S3Client client;
  private final String bucket;

  private static final int PART_NUMBER_ONE = 1;

  private static final int PART_NUMBER_TWO = 2;

  AwsS3Client(S3ClientProperties s3ClientProperties, S3Client client) {
    super(s3ClientProperties);
    this.client = client;
    bucket = s3ClientProperties.getBucket();
  }

  public AwsS3Client(S3ClientProperties s3ClientProperties) {
    this(s3ClientProperties, createS3Client(s3ClientProperties));
  }

  static S3Client createS3Client(S3ClientProperties s3ClientProperties) {
    final String accessKey = s3ClientProperties.getAccessKey();
    final String endpoint = s3ClientProperties.getEndpoint();
    final String secretKey = s3ClientProperties.getSecretKey();
    final String region = s3ClientProperties.getRegion();
    final String bucket = s3ClientProperties.getBucket();

    log.info("Creating AWS SDK client endpoint {},region {},bucket {},accessKey {},secretKey {}.", endpoint, region, bucket,
        StringUtils.isNotBlank(accessKey) ? "<set>" : "<not set>", StringUtils.isNotBlank(secretKey) ? "<set>" : "<not set>");

    AwsCredentialsProvider credentialsProvider;

    if (StringUtils.isNotBlank(accessKey) && StringUtils.isNotBlank(secretKey)) {
      var awsCredentials = AwsBasicCredentials.create(accessKey, secretKey);
      credentialsProvider = StaticCredentialsProvider.create(awsCredentials);
    } else {
      credentialsProvider = DefaultCredentialsProvider.create();
    }

    return S3Client.builder()
        .endpointOverride(URI.create(endpoint))
        .forcePathStyle(s3ClientProperties.isForcePathStyle())
        .region(Region.of(region))
        .credentialsProvider(credentialsProvider)
        .build();
  }

  @Override
  public String write(String path, InputStream is) {
    log.debug("Writing with using AWS SDK client");
    try (is) {
       var size = is.available();
       client.putObject(PutObjectRequest.builder()
        .bucket(bucket)
        .key(path)
        .build(), RequestBody.fromInputStream(is, size));
      return path;
    } catch (Exception e) {
      throw new S3ClientException("Cannot write file: " + path, e);
    }
  }

  /**
   * {@code @deprecated, won't be used in future}
   */
  @Deprecated(forRemoval = true)
  @Override
  public String append(String path, InputStream is) {
    log.debug("Appending with using AWS SDK client");
    String uploadId = null;
    try (is) {
      if (list(path).isEmpty()) {
        log.debug("Appending non-existing file");
        return write(path, is);
      } else {
        var size = getSize(path);
        log.debug("Appending to {} with size {}", path, size);

        if (size > MIN_MULTIPART_SIZE) {

          var createMultipartUploadRequest = CreateMultipartUploadRequest.builder()
            .bucket(bucket)
            .key(path)
            .build();

          uploadId = client.createMultipartUpload(createMultipartUploadRequest)
            .uploadId();

          var uploadPartRequest1 = UploadPartCopyRequest.builder()
            .sourceBucket(bucket)
            .sourceKey(path)
            .uploadId(uploadId)
            .destinationBucket(bucket)
            .destinationKey(path)
            .partNumber(PART_NUMBER_ONE)
            .build();

          var uploadPartRequest2 = UploadPartRequest.builder()
            .bucket(bucket)
            .key(path)
            .uploadId(uploadId)
            .partNumber(PART_NUMBER_TWO)
            .build();

          var originalEtag = client.uploadPartCopy(uploadPartRequest1)
            .copyPartResult()
            .eTag();
          var appendedEtag = client.uploadPart(uploadPartRequest2, RequestBody.fromInputStream(is, is.available()))
            .eTag();

          var original = CompletedPart.builder()
            .partNumber(PART_NUMBER_ONE)
            .eTag(originalEtag)
            .build();
          var appended = CompletedPart.builder()
            .partNumber(PART_NUMBER_TWO)
            .eTag(appendedEtag)
            .build();

          var completedMultipartUpload = CompletedMultipartUpload.builder()
            .parts(original, appended)
            .build();

          var completeMultipartUploadRequest = CompleteMultipartUploadRequest.builder()
            .bucket(bucket)
            .key(path)
            .uploadId(uploadId)
            .multipartUpload(completedMultipartUpload)
            .build();

          return client.completeMultipartUpload(completeMultipartUploadRequest)
            .key();

        } else {
          var original = read(path);
          var composed = new SequenceInputStream(original, is);
          return write(path, composed);
        }
      }
    } catch (Exception e) {
      if (uploadId != null) {
        try {
          client.abortMultipartUpload(AbortMultipartUploadRequest.builder()
              .bucket(bucket)
              .key(path)
              .uploadId(uploadId)
              .build());
        } catch (Exception e2) {
          // ignore, because it is most likely the same as e (eg. network problem)
        }
      }
      log.error("Cannot append data for path: {}", path, e);
      throw new S3ClientException("Cannot append data for path: " + path, e);
    }
  }
}
