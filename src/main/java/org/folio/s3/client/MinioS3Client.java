package org.folio.s3.client;

import static io.minio.ObjectWriteArgs.MAX_PART_SIZE;
import static io.minio.ObjectWriteArgs.MIN_MULTIPART_SIZE;

import com.google.common.collect.ImmutableMultimap;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import io.minio.StatObjectArgs;
import org.apache.commons.lang3.StringUtils;
import org.folio.s3.client.impl.ExtendedMinioAsyncClient;
import org.folio.s3.exception.S3ClientException;

import io.minio.BucketExistsArgs;
import io.minio.GetObjectArgs;
import io.minio.ListObjectsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioAsyncClient;
import io.minio.PutObjectArgs;
import io.minio.RemoveObjectArgs;
import io.minio.RemoveObjectsArgs;
import io.minio.UploadObjectArgs;
import io.minio.credentials.IamAwsProvider;
import io.minio.credentials.Provider;
import io.minio.credentials.StaticProvider;
import io.minio.messages.DeleteObject;
import io.minio.messages.Part;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class MinioS3Client implements FolioS3Client {

  private final ExtendedMinioAsyncClient client;
  private final String bucket;
  private final String region;

  MinioS3Client(S3ClientProperties properties, ExtendedMinioAsyncClient client) {
    region = properties.getRegion();
    bucket = properties.getBucket();
    this.client = client;
    createBucketIfNotExists();
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

  @SuppressWarnings("java:S2142")  // we wrap and rethrow InterruptedException
  void createBucketIfNotExists() {
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

  @SuppressWarnings("java:S2142")  // we wrap and rethrow InterruptedException
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

  @SuppressWarnings("java:S2142")  // we wrap and rethrow InterruptedException
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

      var multipart = client.createMultipartUploadAsync(bucket, region, path, null, null).get().result();
      uploadId = multipart.uploadId();
      var source = URLEncoder.encode(bucket + "/" + path, StandardCharsets.UTF_8);
      var header = ImmutableMultimap.of("x-amz-copy-source", source);
      var part1 = client.uploadPartCopyAsync(bucket, region, path, uploadId, 1, header, null);
      var part2 = client.putObject(PutObjectArgs.builder().bucket(bucket).region(region).object(path)
          .stream(is, -1, MAX_PART_SIZE)
          .extraQueryParams(Map.of("uploadId", uploadId, "partNumber", "2")).build());
      Part [] parts = {
          new Part(1, part1.get().result().etag()),
          new Part(2, part2.get().etag())
          };
      var result = client.completeMultipartUploadAsync(bucket, region, path, uploadId, parts, null, null).get();
      return result.object();
    } catch (Exception e) {
      if (uploadId != null) {
        try {
          client.abortMultipartUploadAsync(bucket, region, path, uploadId, null, null);
        } catch (Exception e2) {
          // ignore
        }
      }
      throw new S3ClientException("Cannot append data for path: " + path, e);
    }
  }

  @SuppressWarnings("java:S2142")  // we wrap and rethrow InterruptedException
  @Override
  public String write(String path, InputStream is) {
    log.debug("Writing with using Minio client");
    try (is) {
      return client.putObject(PutObjectArgs.builder()
        .bucket(bucket)
        .region(region)
        .object(path)
        // use max part size to not exceed the maximum number of parts (10000)
        .stream(is, -1, MAX_PART_SIZE)
        .build())
        .get()
        .object();
    } catch (Exception e) {
      throw new S3ClientException("Cannot write stream: " + path, e);
    }
  }

  @SuppressWarnings("java:S2142")  // we wrap and rethrow InterruptedException
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

  @SuppressWarnings("java:S2142")  // we wrap and rethrow InterruptedException
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

  @SuppressWarnings("java:S2142")  // we wrap and rethrow InterruptedException
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
}
