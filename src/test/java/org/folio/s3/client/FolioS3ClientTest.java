package org.folio.s3.client;

import static io.minio.ObjectWriteArgs.MIN_MULTIPART_SIZE;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.folio.s3.client.impl.ExtendedMinioAsyncClient;
import org.folio.s3.exception.S3ClientException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;

import com.google.common.collect.Multimap;

import io.minio.AbortMultipartUploadResponse;
import io.minio.ObjectWriteResponse;
import io.minio.PutObjectArgs;
import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.UploadPartCopyRequest;
import software.amazon.awssdk.services.s3.model.UploadPartCopyResponse;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

@Log4j2
class FolioS3ClientTest {
  public static final int S3_PORT = 9000;
  public static final String S3_BUCKET = "test-bucket";
  public static final String S3_REGION = "us-west-2";
  private static GenericContainer<?> s3;
  public static final String S3_ACCESS_KEY = "minio-access-key";
  public static final String S3_SECRET_KEY = "minio-secret-key";
  private static final int SMALL_SIZE = 1024;
  public static final int LARGE_SIZE = MIN_MULTIPART_SIZE + 1;

  private static String endpoint;
  private static FolioS3Client s3Client;

  @BeforeAll
  public static void setUp() {
    s3 = new GenericContainer<>("minio/minio:latest").withEnv("MINIO_ACCESS_KEY", S3_ACCESS_KEY)
      .withEnv("MINIO_SECRET_KEY", S3_SECRET_KEY)
      .withCommand("server /data")
      .withExposedPorts(S3_PORT)
      .waitingFor(new HttpWaitStrategy().forPath("/minio/health/ready")
        .forPort(S3_PORT)
        .withStartupTimeout(Duration.ofSeconds(10)));
    s3.start();

    endpoint = format("http://%s:%s", s3.getHost(), s3.getFirstMappedPort());

  }

  @AfterAll
  public static void tearDown() {
    s3.stop();
  }

  @ParameterizedTest
  @ValueSource(booleans = { true, false })
  void testWriteReadDeleteFile(boolean isAwsSdk) throws IOException {
    s3Client = S3ClientFactory.getS3Client(getS3ClientProperties(isAwsSdk, endpoint));
    byte[] content = getRandomBytes(SMALL_SIZE);
    var original = List.of("directory_1/CSV_Data_1.csv", "directory_1/directory_2/CSV_Data_2.csv",
        "directory_1/directory_2/directory_3/CSV_Data_3.csv");

    // Write files content
    List<String> expected;
    try {
      expected = original.stream()
        .map(p -> s3Client.write(p, new ByteArrayInputStream(content)))
        .collect(toList());
    } catch (Exception e) {
      throw new IOException(e);
    }

    assertTrue(Objects.deepEquals(original, expected));

    assertTrue(
        Objects.deepEquals(s3Client.list("directory_1/"), List.of("directory_1/CSV_Data_1.csv", "directory_1/directory_2/")));

    assertTrue(Objects.deepEquals(s3Client.list("directory_1/directory_2/"),
        List.of("directory_1/directory_2/CSV_Data_2.csv", "directory_1/directory_2/directory_3/")));

    // Read files content
    original.forEach(p -> {
      try (var is = s3Client.read(p)) {
        assertTrue(Objects.deepEquals(content, is.readAllBytes()));
        var link = s3Client.getPresignedUrl(p);
        assertNotNull(link);
        assertTrue(link.contains(p));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });


    // Remove files files
    String[] paths = new String[original.size()];
    original.toArray(paths);
    s3Client.remove(paths);
    assertEquals(0, s3Client.list("directory_1/")
      .size());
  }

  @ParameterizedTest
  @ValueSource(booleans = { true, false })
  void testUploadReadDeleteFile(boolean isAwsSdk) throws IOException {
    s3Client = S3ClientFactory.getS3Client(getS3ClientProperties(isAwsSdk, endpoint));
    byte[] content = getRandomBytes(SMALL_SIZE);
    var fileOnStorage = "directory_1/CSV_Data_1.csv";

    var tempFileName = "content.csv";
    var tempFilePath = Paths.get(tempFileName);

    Files.deleteIfExists(tempFilePath);
    Files.createFile(tempFilePath);
    Files.write(tempFilePath, content);

    // Upload files content
    s3Client.upload(tempFilePath.toString(), fileOnStorage);

    assertEquals(1, s3Client.list("directory_1/")
      .size());
    assertEquals("directory_1/CSV_Data_1.csv", s3Client.list("directory_1/")
      .get(0));

    // Read files content
    try (var is = s3Client.read(fileOnStorage)) {
      assertTrue(Objects.deepEquals(content, is.readAllBytes()));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    // Remove files files
    s3Client.remove(fileOnStorage);
    assertEquals("[]", s3Client.list("directory_1/")
      .toString());

    Files.deleteIfExists(tempFilePath);
  }

  @Deprecated
  @ParameterizedTest
  @CsvSource({ "true," + SMALL_SIZE, "true," + LARGE_SIZE, "false," + SMALL_SIZE, "false," + LARGE_SIZE })
  void testAppendFile(boolean isAwsSdk, int size) throws IOException {
    s3Client = S3ClientFactory.getS3Client(getS3ClientProperties(isAwsSdk, endpoint));
    byte[] content1 = getRandomBytes(size);
    byte[] content2 = getRandomBytes(size + 1);
    var source = "directory_1/CSV_Data_1.csv";

    // Append to non-existing source
    s3Client.append(source, new ByteArrayInputStream(content1));

    // Append to existing source
    s3Client.append(source, new ByteArrayInputStream(content2));

    try (var is = s3Client.read(source)) {
      assertTrue(Objects.deepEquals(ArrayUtils.addAll(content1, content2), is.readAllBytes()));
    }

    s3Client.remove(source);
  }

  @Deprecated
  @Disabled
  @Test
  void testAppendAbortMinio() {
    var path = "appendAbort.txt";
    byte[] content = getRandomBytes(LARGE_SIZE);
    var properties = getS3ClientProperties(false, endpoint);
    AtomicBoolean aborted = new AtomicBoolean(false);
    var mock = new ExtendedMinioAsyncClient(MinioS3Client.createClient(properties)) {
      @SneakyThrows
      public CompletableFuture<ObjectWriteResponse> putObject(PutObjectArgs args) {
        if (args.extraQueryParams()
          .isEmpty()) {
          return super.putObject(args);
        }
        throw new NegativeArraySizeException("greetings from mock");
      }

      @SneakyThrows
      public CompletableFuture<AbortMultipartUploadResponse> abortMultipartUploadAsync(String bucketName, String region,
          String objectName, String uploadId, Multimap<String, String> extraHeaders, Multimap<String, String> extraQueryParams) {
        aborted.set(true);
        return super.abortMultipartUploadAsync(bucketName, region, objectName, uploadId, extraHeaders, extraQueryParams);
      }
    };
    s3Client = new MinioS3Client(properties, mock);
    s3Client.write(path, new ByteArrayInputStream(content));
    var stream = new ByteArrayInputStream(content);
    var e = assertThrows(S3ClientException.class, () -> s3Client.append(path, stream));
    assertEquals("greetings from mock", e.getCause()
      .getMessage());
    assertTrue(aborted.get());
  }

  @Deprecated
  @Test
  void testAppendAbortAws() {
    var path = "appendAbort.txt";
    byte[] content = getRandomBytes(LARGE_SIZE);
    var properties = getS3ClientProperties(true, endpoint);
    AtomicBoolean aborted = new AtomicBoolean(false);
    var aws = AwsS3Client.createS3Client(properties);
    var mock = new S3Client() {
      public void close() {
      }

      public String serviceName() {
        return "serviceName";
      }

      public CreateMultipartUploadResponse createMultipartUpload(CreateMultipartUploadRequest request) {
        return aws.createMultipartUpload(request);
      }

      public UploadPartCopyResponse uploadPartCopy(UploadPartCopyRequest request) {
        return aws.uploadPartCopy(request);
      }

      public UploadPartResponse uploadPart(UploadPartRequest uploadPartRequest, RequestBody requestBody) {
        throw new UnsupportedOperationException("greetings from mock");
      }

      public software.amazon.awssdk.services.s3.model.AbortMultipartUploadResponse abortMultipartUpload(
          AbortMultipartUploadRequest request) {
        aborted.set(true);
        return aws.abortMultipartUpload(request);
      }
    };
    s3Client = new AwsS3Client(properties, aws);
    s3Client.write(path, new ByteArrayInputStream(content));
    var mockClient = new AwsS3Client(properties, mock);
    var stream = new ByteArrayInputStream(content);
    var e = assertThrows(S3ClientException.class, () -> mockClient.append(path, stream));
    assertEquals("greetings from mock", e.getCause()
      .getMessage());
    assertTrue(aborted.get());
  }

  @DisplayName("Files operations on non-existing file")
  @ParameterizedTest
  @ValueSource(booleans = { true, false })
  void testNonExistingFileOperations(boolean isAwsSdk) {
    s3Client = S3ClientFactory.getS3Client(getS3ClientProperties(isAwsSdk, endpoint));
    var fakeLocalPath = "/fake-local-path";
    var fakeRemotePath = "/fake-remote-path";
    // upload
    assertThrows(S3ClientException.class, () -> s3Client.upload(fakeLocalPath, fakeLocalPath));

    // compose
//    assertThrows(S3ClientException.class, () -> s3Client.append(fakeRemotePath, new ByteArrayInputStream(new byte[0])));

    // write
    assertThrows(S3ClientException.class, () -> s3Client.write(fakeLocalPath, null));

    // remove
    assertThrows(S3ClientException.class, () -> s3Client.remove(StringUtils.EMPTY));
    assertTrue(s3Client.remove(new String[0])
      .isEmpty());

    // read
    assertThrows(S3ClientException.class, () -> s3Client.read(fakeRemotePath));

    // list
    assertTrue(s3Client.list(fakeRemotePath)
      .isEmpty());
  }

  @ParameterizedTest
  @ValueSource(ints = {SMALL_SIZE, LARGE_SIZE})
  void testRemoteStorageWriter(int size) throws IOException {
    final String path = "opt-writer/test.txt";

    s3Client = S3ClientFactory.getS3Client(getS3ClientProperties(false, endpoint));
    final var data = getRandomBytes(size);

    try (final var writer = s3Client.getRemoteStorageWriter(path, 5 * SMALL_SIZE)) {
      writer.write(new String(data));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    try (var is = s3Client.read(path)) {
      assertTrue(Objects.deepEquals(new String(data), new String(is.readAllBytes())));
    }
  }

  @Test
  void testFailsRemoteStorageWriter() {
    final String path = "";
    final int size = 0;
    final FolioS3Client client = S3ClientFactory.getS3Client(getS3ClientProperties(false, endpoint));

    assertThrows(S3ClientException.class, () -> client.getRemoteStorageWriter(path, size));
  }

  // TODO: delete isAwsSdk in the future because of AWS S3 will be unsupported
  public static S3ClientProperties getS3ClientProperties(boolean isAwsSdk, String endpoint) {
    return S3ClientProperties.builder()
      .endpoint(endpoint)
      .forcePathStyle(true)
      .secretKey(S3_SECRET_KEY)
      .accessKey(S3_ACCESS_KEY)
      .bucket(S3_BUCKET)
      .awsSdk(isAwsSdk)
      .region(S3_REGION)
      .build();
  }

  public static byte[] getRandomBytes(int size) {
    var original = new byte[size];
    ThreadLocalRandom.current()
      .nextBytes(original);
    return original;
  }
}
