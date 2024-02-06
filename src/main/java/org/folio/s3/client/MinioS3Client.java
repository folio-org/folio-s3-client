package org.folio.s3.client;

import static io.minio.ObjectWriteArgs.MAX_PART_SIZE;
import static io.minio.ObjectWriteArgs.MIN_MULTIPART_SIZE;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import io.minio.BucketExistsArgs;
import io.minio.GetObjectArgs;
import io.minio.GetPresignedObjectUrlArgs;
import io.minio.ListObjectsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioAsyncClient;
import io.minio.PutObjectArgs;
import io.minio.RemoveObjectArgs;
import io.minio.RemoveObjectsArgs;
import io.minio.Result;
import io.minio.StatObjectArgs;
import io.minio.UploadObjectArgs;
import io.minio.http.Method;
import io.minio.messages.Item;
import org.apache.commons.lang3.StringUtils;
import org.folio.s3.client.impl.ExtendedMinioAsyncClient;
import org.folio.s3.exception.S3ClientException;

import com.google.common.collect.ImmutableMultimap;

import io.minio.credentials.IamAwsProvider;
import io.minio.credentials.Provider;
import io.minio.credentials.StaticProvider;
import io.minio.messages.DeleteObject;
import io.minio.messages.Part;
import lombok.extern.log4j.Log4j2;

@Log4j2
// 2142: we wrap and rethrow InterruptedException as S3ClientException
// 2221: we want to catch the exceptions from the minio client, but the list is long,
//         so we simply specify `Exception` for simplicity
@SuppressWarnings({"java:S2142", "java:S2221"})
public class MinioS3Client implements FolioS3Client {

  private static final String PARAM_MULTIPART_PART_NUMBER = "partNumber";
  private static final String PARAM_MULTIPART_UPLOAD_ID = "uploadId";
  private static final int EXPIRATION_TIME_IN_MINUTES = 10;
  private final ExtendedMinioAsyncClient client;
  private final String bucket;
  private final String region;

  MinioS3Client(S3ClientProperties properties, ExtendedMinioAsyncClient client) {
    region = properties.getRegion();
    bucket = properties.getBucket();
    this.client = client;
  }

  public MinioS3Client(S3ClientProperties properties) {
    this(properties, createClient(properties));
  }

  static ExtendedMinioAsyncClient createClient(S3ClientProperties properties) {
    final String accessKey = properties.getAccessKey();
    final String secretKey = properties.getSecretKey();
    final String endpoint = properties.getEndpoint();
    final String region = properties.getRegion();
    final String bucket = properties.getBucket();

    log.info("Creating MinIO client endpoint {},region {},bucket {},accessKey {},secretKey {}.", endpoint, region, bucket,
        StringUtils.isNotBlank(accessKey) ? "<set>" : "<not set>", StringUtils.isNotBlank(secretKey) ? "<set>" : "<not set>");

    var builder = MinioAsyncClient.builder()
      .endpoint(endpoint);
    if (StringUtils.isNotBlank(region)) {
      builder.region(region);
    }

    Provider provider;
    if (StringUtils.isNotBlank(accessKey) && StringUtils.isNotBlank(secretKey)) {
      provider = new StaticProvider(accessKey, secretKey, null);
    } else {
      provider = new IamAwsProvider(null, null);
    }
    log.debug("{} MinIO credentials provider created.", provider.getClass()
      .getSimpleName());
    builder.credentialsProvider(provider);
    return ExtendedMinioAsyncClient.build(builder);
  }

  public void createBucketIfNotExists() {
    try {
      if (StringUtils.isBlank(bucket)) {
        log.debug("Bucket name is null, empty or blank.");
        return;
      }
      var exists = client.bucketExists(BucketExistsArgs.builder()
        .bucket(bucket)
        .region(region)
        .build())
        .get();
      if (Boolean.TRUE.equals(exists)) {
        log.debug("Bucket already exists.");
      }
      client.makeBucket(MakeBucketArgs.builder()
        .bucket(bucket)
        .region(region)
        .build());
      log.debug("Created {} bucket.", bucket);
    } catch (Exception e) {
      log.error("Error creating bucket {}: {}", bucket, e.getMessage(), e);
      throw new S3ClientException("Error creating bucket: " + bucket, e);
    }
  }

  private String upload(String path, String filename, Map<String, String> headers) {
    try {
      return client.uploadObject(UploadObjectArgs.builder()
        .bucket(bucket)
        .region(region)
        .object(filename)
        .headers(headers)
        .filename(path)
        .build())
        .get()
        .object();
    } catch (Exception e) {
      throw new S3ClientException("Cannot upload file: " + path, e);
    }
  }

  @Override
  public String upload(String path, String filename) {
    return upload(path, filename, new HashMap<>());
  }

  /**
   * {@code @deprecated, won't be used in future due to unstable work}
   */
  @Deprecated(forRemoval = true)
  @Override
  public String append(String path, InputStream is) {
    String uploadId = null;
    try (is) {
      if (list(path).isEmpty()) {
        log.debug("Appending non-existing file");
        return write(path, is);
      }

      var size = getSize(path);
      log.debug("Appending to {} with size {}", path, size);

      if (size <= MIN_MULTIPART_SIZE) {
        var original = read(path);
        var composed = new SequenceInputStream(original, is);
        return write(path, composed);
      }

      var multipart = client.createMultipartUploadAsync(bucket, region, path, null, null)
        .get()
        .result();
      uploadId = multipart.uploadId();
      var source = URLEncoder.encode(bucket + "/" + path, StandardCharsets.UTF_8);
      var header = ImmutableMultimap.of("x-amz-copy-source", source);
      var part1 = client.uploadPartCopyAsync(bucket, region, path, uploadId, 1, header, null);
      var part2 = client.putObject(PutObjectArgs.builder()
        .bucket(bucket)
        .region(region)
        .object(path)
        .stream(is, -1, MAX_PART_SIZE)
        .extraQueryParams(Map.of(PARAM_MULTIPART_UPLOAD_ID, uploadId, PARAM_MULTIPART_PART_NUMBER, "2"))
        .build());
      Part[] parts = { new Part(1, part1.get()
        .result()
        .etag()), new Part(2,
            part2.get()
              .etag()) };
      var result = client.completeMultipartUploadAsync(bucket, region, path, uploadId, parts, null, null)
        .get();
      return result.object();
    } catch (Exception e) {
      if (uploadId != null) {
        try {
          client.abortMultipartUploadAsync(bucket, region, path, uploadId, null, null);
        } catch (Exception e2) {
          // ignore, because it is most likely the same as e (eg. network problem)
        }
      }
      log.error("Cannot append data for path: {}", path, e);
      throw new S3ClientException("Cannot append data for path: " + path, e);
    }
  }

  @Override
  public String write(String path, InputStream is) {
    log.debug("Writing with using Minio client");
    try (is) {
      return client.putObject(PutObjectArgs.builder()
        .bucket(bucket)
        .region(region)
        .object(path)
        .stream(is, -1, MIN_MULTIPART_SIZE)
        .build())
        .get()
        .object();
    } catch (Exception e) {
      throw new S3ClientException("Cannot write stream: " + path, e);
    }
  }

  @Override
  public String write(String path, InputStream is, long size) {
    return write(path, is);
  }

  @Override
  public String remove(String path) {
    try {
      client.removeObject(RemoveObjectArgs.builder()
        .bucket(bucket)
        .region(region)
        .object(path)
        .build())
        .get();
      return path;
    } catch (Exception e) {
      throw new S3ClientException("Error deleting file: ", e);
    }
  }

  @Override
  public List<String> remove(String... paths) {
    try {

      var errors = client.removeObjects(RemoveObjectsArgs.builder()
        .bucket(bucket)
        .region(region)
        .objects(Arrays.stream(paths)
          .map(DeleteObject::new)
          .collect(Collectors.toList()))
        .build())
        .iterator();

      if (errors.hasNext()) {
        throw new S3ClientException("Error deleting");
      }

      return Arrays.asList(paths);

    } catch (Exception e) {
      throw new S3ClientException("Error deleting file: ", e);
    }
  }

  @Override
  public List<String> list(String path) {
    List<String> list = new ArrayList<>();
    try {
      client.listObjects(ListObjectsArgs.builder()
        .bucket(bucket)
        .region(region)
        .prefix(path)
        .maxKeys(1)
        .build())
        .iterator()
        .forEachRemaining(itemResult -> {
          try {
            list.add(itemResult.get()
              .objectName());
          } catch (Exception e) {
            throw new S3ClientException("Error populating list of objects for path: " + path, e);
          }
        });
      return list;
    } catch (Exception e) {
      throw new S3ClientException("Error getting list of objects for path: " + path, e);
    }
  }

  @Override
  public Iterable<Result<Item>> iterableList(String path, int maxKeys, String startAfter) {
    ListObjectsArgs.Builder listObjectsArgsBuilder = ListObjectsArgs.builder()
            .bucket(this.bucket)
            .region(this.region)
            .prefix(path)
            .maxKeys(maxKeys);

    if (startAfter != null && !startAfter.isEmpty()) {
      listObjectsArgsBuilder.startAfter(startAfter);
    }

    return this.client.listObjects(listObjectsArgsBuilder.build());
  }

  @Override
  public InputStream read(String path) {
    try {
     return client.getObject(GetObjectArgs.builder()
        .bucket(bucket)
        .region(region)
        .object(path)
        .build())
        .get();
    } catch (Exception e) {
      throw new S3ClientException("Error creating input stream for path: " + path, e);
    }
  }

  @Override
  public long getSize(String path) {
    try {
      return client.statObject(StatObjectArgs.builder()
        .bucket(bucket)
        .region(region)
        .object(path)
        .build())
        .get()
        .size();
    } catch (Exception e) {
      throw new S3ClientException("Error getting size: " + path, e);
    }
  }

  @Override
  public RemoteStorageWriter getRemoteStorageWriter(String path, int size) {
    return new RemoteStorageWriter(path, size, this);
  }

  @Override
  public String getPresignedUrl(String path) {
    return getPresignedUrl(path, Method.GET);
  }

  @Override
  public String getPresignedUrl(String path, Method method) {
    try {
      return client.getPresignedObjectUrl(GetPresignedObjectUrlArgs.builder()
        .bucket(bucket)
        .object(path)
        .method(method)
        .expiry(EXPIRATION_TIME_IN_MINUTES, TimeUnit.MINUTES)
        .build());
    } catch (Exception e) {
      throw new S3ClientException(
        "Error getting presigned url for object: " + path + ", method: " + method,
        e
      );
    }
  }

  @Override
  public String initiateMultipartUpload(String path) {
    try {
      return client.createMultipartUploadAsync(bucket, region, path, null, null)
        .get()
        .result()
        .uploadId();
    } catch (Exception e) {
      throw new S3ClientException("Error initiating multipart upload for object: " + path, e);
    }
  }

  @Override
  public String getPresignedMultipartUploadUrl(
    String path,
    String uploadId,
    int partNumber
  ) {
    try {
      return client.getPresignedObjectUrl(GetPresignedObjectUrlArgs.builder()
        .bucket(bucket)
        .object(path)
        .method(Method.PUT)
        .expiry(EXPIRATION_TIME_IN_MINUTES, TimeUnit.MINUTES)
        .extraQueryParams(Map.of(PARAM_MULTIPART_PART_NUMBER, String.valueOf(partNumber), PARAM_MULTIPART_UPLOAD_ID, uploadId))
        .build());
    } catch (Exception e) {
      throw new S3ClientException(
        "Error getting presigned url for part #" + partNumber + "of upload ID: " + uploadId,
        e
      );
    }
  }

  @Override
  public String uploadMultipartPart(
    String path,
    String uploadId,
    int partNumber,
    String filename
  ) {
    try {
      InputStream stream = new FileInputStream(filename);
      return client.putObject(
          PutObjectArgs.builder()
            .bucket(bucket)
            .region(region)
            .object(path)
            .stream(stream, -1, MAX_PART_SIZE)
            .extraQueryParams(Map.of(PARAM_MULTIPART_UPLOAD_ID, uploadId, PARAM_MULTIPART_PART_NUMBER, String.valueOf(partNumber)))
            .build()
        )
        .get()
        .etag();
    } catch (Exception e) {
      throw new S3ClientException(
        "Cannot upload part # " + partNumber + " for upload ID: " + uploadId,
        e
      );
    }
  }

  @Override
  public void abortMultipartUpload(
    String path,
    String uploadId
  ) {
    try {
       client.abortMultipartUploadAsync(bucket, region, path, uploadId, null, null).get();
    } catch (Exception e) {
      throw new S3ClientException(
        "Error getting presigned url for upload ID: " + uploadId,
        e
      );
    }
  }

  @Override
  public void completeMultipartUpload(
    String path,
    String uploadId,
    List<String> partETags
  ) {
    try {
      client.completeMultipartUploadAsync(
        bucket,
        region,
        path,
        uploadId,
        IntStream.range(0, partETags.size())
          .mapToObj(i -> new Part(i + 1, partETags.get(i)))
          .toArray(Part[]::new),
        null,
        null
      ).get();
    } catch (Exception e) {
      throw new S3ClientException(
        "Error getting presigned url for upload ID: " + uploadId,
        e
      );
    }
  }
}
