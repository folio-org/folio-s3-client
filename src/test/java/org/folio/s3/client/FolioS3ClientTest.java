package org.folio.s3.client;

import static io.minio.ObjectWriteArgs.MIN_MULTIPART_SIZE;
import static java.lang.String.format;
import static java.time.temporal.ChronoUnit.MINUTES;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.S3;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.IntStream;

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
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;

import com.google.common.collect.Multimap;

import io.minio.AbortMultipartUploadResponse;
import io.minio.ObjectWriteResponse;
import io.minio.PutObjectArgs;
import io.minio.http.Method;
import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.UploadPartCopyRequest;
import software.amazon.awssdk.services.s3.model.UploadPartCopyResponse;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

@Log4j2
class FolioS3ClientTest {
  private static LocalStackContainer localstack;
  public static String region;
  public static String accessKey;
  public static String secretKey;
  public static final String S3_BUCKET = "test-bucket";
  private static final int SMALL_SIZE = 1024;
  public static final int LARGE_SIZE = MIN_MULTIPART_SIZE + 1;

  private static String endpoint;

  @BeforeAll
  public static void setUp() {

    DockerImageName localstackImage = DockerImageName.parse("localstack/localstack:3.3.0");

    localstack = new LocalStackContainer(localstackImage)
            .withStartupTimeout(Duration.of(5, MINUTES))
            .withServices(S3);

    accessKey = localstack.getAccessKey();
    secretKey = localstack.getSecretKey();
    region = localstack.getRegion();

    localstack.start();

    endpoint = format("http://%s:%s", localstack.getHost(), localstack.getFirstMappedPort());
  }

  @AfterAll
  public static void tearDown() {
    localstack.stop();
  }

  @ParameterizedTest
  @ValueSource(booleans = { true, false })
  void testWriteReadDeleteFile(boolean isAwsSdk) throws IOException {
    var properties = getS3ClientProperties(isAwsSdk, endpoint);
    var s3Client = S3ClientFactory.getS3Client(properties);
    s3Client.createBucketIfNotExists();
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
  void testWriteByStream(boolean isAwsSdk) throws IOException {
    var properties = getS3ClientProperties(isAwsSdk, endpoint);
    var s3Client = S3ClientFactory.getS3Client(properties);

    s3Client.createBucketIfNotExists();

    byte[] content = getRandomBytes(SMALL_SIZE);
    String original = "directory_1/CSV_Data_1.csv";

    // Write file content
    String expected;
    try {
      InputStream is = new ByteArrayInputStream(content);
      expected = s3Client.write(original, is, is.available());
    } catch (Exception e) {
      throw new IOException(e);
    }

    assertEquals(original, expected);

    assertTrue(
        Objects.deepEquals(s3Client.list("directory_1/"), List.of("directory_1/CSV_Data_1.csv")));

    // Read file content
    try (var is = s3Client.read(expected)) {
      assertTrue(Objects.deepEquals(content, is.readAllBytes()));
      var link = s3Client.getPresignedUrl(expected);
      assertNotNull(link);
      assertTrue(link.contains(expected));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    // Remove file
    s3Client.remove(expected);
  }

  @ParameterizedTest
  @ValueSource(booleans = { true, false })
  void testUploadReadDeleteFile(boolean isAwsSdk) throws IOException {
    var properties = getS3ClientProperties(isAwsSdk, endpoint);
    var s3Client = S3ClientFactory.getS3Client(properties);
    s3Client.createBucketIfNotExists();
    byte[] content = getRandomBytes(SMALL_SIZE);
    var fileOnStorage = "directory_1/CSV_Data_1.csv";

    var tempFileName = "content.csv";
    var tempFilePath = Paths.get(tempFileName);

    Files.deleteIfExists(tempFilePath);
    Files.createFile(tempFilePath);
    Files.write(tempFilePath, content);

    var link = s3Client.getPresignedUrl(fileOnStorage, Method.PUT);
    assertNotNull(link);
    assertTrue(link.contains(fileOnStorage));

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
    var properties = getS3ClientProperties(isAwsSdk, endpoint);
    var s3Client = S3ClientFactory.getS3Client(properties);
    s3Client.createBucketIfNotExists();
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
    var s3Client = new MinioS3Client(properties, mock);
    s3Client.createBucketIfNotExists();
    s3Client.write(path, new ByteArrayInputStream(content));
    var stream = new ByteArrayInputStream(content);
    var e = assertThrows(S3ClientException.class, () -> s3Client.append(path, stream));
    assertEquals("greetings from mock", e.getCause()
      .getMessage());
    assertTrue(aborted.get());
  }

  @Deprecated
  @Disabled
  @Test
  void testAppendAbortAws() {
    var path = "appendAbort.txt";
    byte[] content = getRandomBytes(LARGE_SIZE);
    var properties = getS3ClientProperties(true, endpoint);
    AtomicBoolean aborted = new AtomicBoolean(false);
    var aws = AwsS3Client.createS3Client(properties);
    var mock = new S3AsyncClient() {
      public void close() {
      }

      public String serviceName() {
        return "serviceName";
      }

      public CompletableFuture<CreateMultipartUploadResponse> createMultipartUpload(CreateMultipartUploadRequest request) {
        return aws.createMultipartUpload(request);
      }

      public CompletableFuture<UploadPartCopyResponse> uploadPartCopy(UploadPartCopyRequest request) {
        return aws.uploadPartCopy(request);
      }

      public UploadPartResponse uploadPart(UploadPartRequest uploadPartRequest, RequestBody requestBody) {
        throw new UnsupportedOperationException("greetings from mock");
      }

        public CompletableFuture<software.amazon.awssdk.services.s3.model.AbortMultipartUploadResponse> abortMultipartUpload(Consumer<AbortMultipartUploadRequest.Builder> abortMultipartUploadRequest) {
            aborted.set(true);
          return S3AsyncClient.super.abortMultipartUpload(abortMultipartUploadRequest);
        }
    };
    var s3Client = new AwsS3Client(properties, aws);
    s3Client.createBucketIfNotExists();
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
    var properties = getS3ClientProperties(isAwsSdk, endpoint);
    var s3Client = S3ClientFactory.getS3Client(properties);
    s3Client.createBucketIfNotExists();
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

    var properties = getS3ClientProperties(false, endpoint);
    var s3Client = S3ClientFactory.getS3Client(properties);
    s3Client.createBucketIfNotExists();
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

  @ParameterizedTest
  @ValueSource(booleans = { true, false })
  void testMultipart(boolean isAwsSdk) throws IOException {
    S3ClientProperties properties = getS3ClientProperties(isAwsSdk, endpoint);
    FolioS3Client s3Client = S3ClientFactory.getS3Client(properties);
    s3Client.createBucketIfNotExists();

    var fileOnStorage = "directory/file.ext";

    List<byte[]> contents = Arrays.asList(
        getRandomBytes(LARGE_SIZE),
        getRandomBytes(LARGE_SIZE),
        getRandomBytes(SMALL_SIZE));
    List<Path> tempFilePaths = Arrays.asList(
        Paths.get("part1"),
        Paths.get("part2"),
        Paths.get("part3"));

    IntStream.range(0, 3).forEach(i -> {
      try {
        Files.deleteIfExists(tempFilePaths.get(i));
        Files.createFile(tempFilePaths.get(i));
        Files.write(tempFilePaths.get(i), contents.get(i));
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    });

    // start upload
    String uploadId = s3Client.initiateMultipartUpload(fileOnStorage);
    assertNotNull(uploadId);

    List<String> eTags = IntStream.rangeClosed(1, 3).mapToObj(i -> {
      // get presigned URLs
      String link = s3Client.getPresignedMultipartUploadUrl(fileOnStorage, uploadId, i);
      assertNotNull(link);
      assertTrue(link.contains("partNumber=" + i));
      assertTrue(link.contains(fileOnStorage));

      // upload it (normal way)
      String eTag = s3Client.uploadMultipartPart(
          fileOnStorage,
          uploadId,
          i,
          tempFilePaths.get(i - 1).toString());
      assertNotNull(eTag);
      return eTag;
    }).collect(toList());

    // complete upload
    s3Client.completeMultipartUpload(fileOnStorage, uploadId, eTags);

    // too late to abort
    assertThrows(S3ClientException.class, () -> s3Client.abortMultipartUpload(fileOnStorage, uploadId));

    assertEquals(1, s3Client.list("directory/").size());
    assertEquals("directory/file.ext", s3Client.list("directory/").get(0));

    // Read files content
    try (InputStream is = s3Client.read(fileOnStorage)) {
      byte[] fromS3 = is.readAllBytes();
      assertTrue(Objects.deepEquals(contents.get(0), Arrays.copyOfRange(fromS3, 0, LARGE_SIZE)));
      assertTrue(Objects.deepEquals(contents.get(1), Arrays.copyOfRange(fromS3, LARGE_SIZE, LARGE_SIZE * 2)));
      assertTrue(Objects.deepEquals(contents.get(2), Arrays.copyOfRange(fromS3, LARGE_SIZE * 2, LARGE_SIZE * 2 + SMALL_SIZE)));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    // Remove files files
    s3Client.remove(fileOnStorage);
    assertEquals("[]", s3Client.list("directory/").toString());

    tempFilePaths.forEach(path -> {
      try {
        Files.deleteIfExists(path);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    });
  }

  @ParameterizedTest
  @ValueSource(booleans = { true, false })
  void testListObjects(boolean isAwsSdk) throws IOException {
    // Setup
    var properties = getS3ClientProperties(isAwsSdk, endpoint);
    var s3Client = S3ClientFactory.getS3Client(properties);
    s3Client.createBucketIfNotExists();

    // Upload some objects to the bucket
    byte[] content = getRandomBytes(SMALL_SIZE);
    List<String> expectedObjects = Arrays.asList("object1.txt", "object2.txt", "object3.txt");

    for (String objectKey : expectedObjects) {
      try {
        s3Client.write(objectKey, new ByteArrayInputStream(content));
      } catch (Exception e) {
        throw new IOException(e);
      }
    }

    // Call the list method and handle exceptions
    List<String> actualObjects;
    try {
      actualObjects = s3Client.list("", 1000, null);
    } catch (Exception e) {
      // Handle any other exceptions that might occur during list
      e.printStackTrace();
      actualObjects = Collections.emptyList();
    }

    // Assertions
    assertEquals(expectedObjects, actualObjects);

    // Clean up - Remove the test objects
    s3Client.remove(expectedObjects.toArray(new String[0]));
  }

  @ParameterizedTest
  @ValueSource(booleans = { true, false })
  void testListObjectsWithStartAfter(boolean isAwsSdk) throws IOException {
    // Setup
    var properties = getS3ClientProperties(isAwsSdk, endpoint);
    var s3Client = S3ClientFactory.getS3Client(properties);
    s3Client.createBucketIfNotExists();

    // Upload some objects to the bucket
    byte[] content = getRandomBytes(SMALL_SIZE);
    List<String> expectedObjects = Arrays.asList("object1.txt", "object2.txt", "object3.txt");

    for (String objectKey : expectedObjects) {
      try {
        s3Client.write(objectKey, new ByteArrayInputStream(content));
      } catch (Exception e) {
        throw new IOException(e);
      }
    }

    // Call the list method with non-null and non-empty startAfter
    List<String> actualObjects;
    try {
      String startAfterKey = "object2.txt";
      actualObjects = s3Client.list("", 1000, startAfterKey);
    } catch (Exception e) {
      // Handle any other exceptions that might occur during list
      e.printStackTrace();
      actualObjects = Collections.emptyList();
    }

    // Assertions
    assertEquals(expectedObjects.subList(2, 3), actualObjects); // Only expect the objects after startAfterKey

    // Clean up - Remove the test objects
    s3Client.remove(expectedObjects.toArray(new String[0]));
  }

  @ParameterizedTest
  @ValueSource(booleans = { true, false })
  void testMultipartExceptions(boolean isAwsSdk) throws IOException {
    S3ClientProperties properties = getS3ClientProperties(isAwsSdk, endpoint);
    FolioS3Client s3Client = S3ClientFactory.getS3Client(properties);
    s3Client.createBucketIfNotExists();

    var fileOnStorage = "directory/file.ext";

    byte[] content = getRandomBytes(LARGE_SIZE);
    Path tempFilePath = Paths.get("part1");
    String tempFilePathString = tempFilePath.toString();

    Files.deleteIfExists(tempFilePath);
    Files.createFile(tempFilePath);
    Files.write(tempFilePath, content);

    // start upload
    String uploadId = s3Client.initiateMultipartUpload(fileOnStorage);
    assertNotNull(uploadId);

    // abort upload
    s3Client.abortMultipartUpload(fileOnStorage, uploadId);

    // now, all further operations should fail...
    assertThrows(
        S3ClientException.class,
        () -> s3Client.uploadMultipartPart(
            fileOnStorage,
            uploadId,
            1,
            tempFilePathString));

    List<String> emptyList = new ArrayList<>();
    assertThrows(
        S3ClientException.class,
        () -> s3Client.completeMultipartUpload(fileOnStorage, uploadId, emptyList));

    // the presigned URL will always generate successfully, only failing later on upload
    // we'll give a bad parameters to simulate failure
    assertThrows(
        S3ClientException.class,
        () -> s3Client.getPresignedMultipartUploadUrl(null, null, 1));

    // and to check that a bad filename results in failure
    assertThrows(
        S3ClientException.class,
        () -> s3Client.initiateMultipartUpload(""));

    // nothing should have been saved
    assertEquals("[]", s3Client.list("directory/").toString());

    Files.deleteIfExists(tempFilePath);
  }

  // TODO: delete isAwsSdk in the future because of AWS S3 will be unsupported
  public static S3ClientProperties getS3ClientProperties(boolean isAwsSdk, String endpoint) {
    return S3ClientProperties.builder()
      .endpoint(endpoint)
      .forcePathStyle(true)
      .secretKey(secretKey)
      .accessKey(accessKey)
      .bucket(S3_BUCKET)
      .awsSdk(isAwsSdk)
      .region(region)
      .build();
  }

  public static byte[] getRandomBytes(int size) {
    var original = new byte[size];
    ThreadLocalRandom.current()
      .nextBytes(original);
    return original;
  }
}
