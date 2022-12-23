package org.folio.s3.client;

import static io.minio.ObjectWriteArgs.MIN_MULTIPART_SIZE;

import java.io.InputStream;
import java.io.SequenceInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import io.minio.StatObjectArgs;
import org.apache.commons.lang3.StringUtils;
import org.folio.s3.exception.S3ClientException;

import io.minio.BucketExistsArgs;
import io.minio.ComposeObjectArgs;
import io.minio.ComposeSource;
import io.minio.GetObjectArgs;
import io.minio.ListObjectsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import io.minio.RemoveObjectArgs;
import io.minio.RemoveObjectsArgs;
import io.minio.UploadObjectArgs;
import io.minio.credentials.IamAwsProvider;
import io.minio.credentials.Provider;
import io.minio.credentials.StaticProvider;
import io.minio.messages.DeleteObject;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class MinioS3Client implements FolioS3Client {

  private final MinioClient client;
  private final String bucket;
  private final String region;

  public MinioS3Client(S3ClientProperties properties) {

    final String accessKey = properties.getAccessKey();
    final String secretKey = properties.getSecretKey();
    final String endpoint = properties.getEndpoint();

    region = properties.getRegion();
    bucket = properties.getBucket();

    log.info("Creating MinIO client endpoint {},region {},bucket {},accessKey {},secretKey {}.", endpoint, region, bucket,
        StringUtils.isNotBlank(accessKey) ? "<set>" : "<not set>", StringUtils.isNotBlank(secretKey) ? "<set>" : "<not set>");

    MinioClient.Builder builder = MinioClient.builder()
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

    client = builder.build();

    createBucketIfNotExists();

  }

  void createBucketIfNotExists() {
    try {
      if (StringUtils.isNotBlank(bucket) && !client.bucketExists(BucketExistsArgs.builder()
        .bucket(bucket)
        .region(region)
        .build())) {
        client.makeBucket(MakeBucketArgs.builder()
          .bucket(bucket)
          .region(region)
          .build());
        log.debug("Created {} bucket.", bucket);
      } else {
        log.debug("Bucket has already exist.");
      }
    } catch (Exception e) {
      log.error("Error creating bucket: " + bucket, e);
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
        .object();
    } catch (Exception e) {
      throw new S3ClientException("Cannot upload file: " + path, e);
    }
  }

  @Override
  public String upload(String path, String filename) {
    return upload(path, filename, new HashMap<>());
  }

  @Override
  public String append(String path, InputStream is) {
    try (is) {
      if (list(path).isEmpty()) {
        log.debug("Appending non-existing file");
        return write(path, is);
      } else {
        var size = getSize(path);

        log.debug("Appending to {} with size {}", path, size);
        if (size > MIN_MULTIPART_SIZE) {

          var temporaryFileName = path + "_temp";
          write(temporaryFileName, is);

          try {
            return client.composeObject(ComposeObjectArgs.builder()
              .bucket(bucket)
              .region(region)
              .object(path)
              .sources(List.of(ComposeSource.builder()
                .bucket(bucket)
                .region(region)
                .object(path)
                .build(),
                  ComposeSource.builder()
                    .bucket(bucket)
                    .region(region)
                    .object(temporaryFileName)
                    .build()))
              .build())
              .object();
          } finally {
            remove(temporaryFileName);
          }
        } else {
          var original = read(path);
          var composed = new SequenceInputStream(original, is);
          return write(path, composed);
        }
      }
    } catch (Exception e) {
      throw new S3ClientException("Cannot append data for path: " + path, e);
    }
  }

  private String write(String path, InputStream is, Map<String, String> headers) {
    log.debug("Writing with using Minio client");
    try (is) {
      return client.putObject(PutObjectArgs.builder()
        .bucket(bucket)
        .region(region)
        .object(path)
        .headers(headers)
        .stream(is, -1, MIN_MULTIPART_SIZE)
        .build())
        .object();
    } catch (Exception e) {
      throw new S3ClientException("Cannot write file: " + path, e);
    }
  }

  @Override
  public String write(String path, InputStream is) {
    try (is) {
      return write(path, is, new HashMap<>());
    } catch (Exception e) {
      throw new S3ClientException("Error writing", e);
    }
  }

  @Override
  public String remove(String path) {
    try {
      client.removeObject(RemoveObjectArgs.builder()
        .bucket(bucket)
        .region(region)
        .object(path)
        .build());
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
  public InputStream read(String path) {
    try {
      return client.getObject(GetObjectArgs.builder()
        .bucket(bucket)
        .region(region)
        .object(path)
        .build());
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
        .size();
    } catch (Exception e) {
      throw new S3ClientException("Error getting size: " + path, e);
    }
  }
}
