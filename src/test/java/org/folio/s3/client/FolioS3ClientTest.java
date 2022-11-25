package org.folio.s3.client;

import static io.minio.ObjectWriteArgs.MIN_MULTIPART_SIZE;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.folio.s3.exception.S3ClientException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;

import lombok.extern.log4j.Log4j2;

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

  private static int port;
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

    port = s3.getFirstMappedPort();

  }

  @AfterAll
  public static void tearDown() {
    s3.stop();
  }

  @ParameterizedTest
  @ValueSource(booleans = { true, false })
  void testWriteReadDeleteFile(boolean isAwsSdk) throws IOException {
    s3Client = S3ClientFactory.getS3Client(getS3ClientProperties(isAwsSdk, port));
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
    s3Client = S3ClientFactory.getS3Client(getS3ClientProperties(isAwsSdk, port));
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
    assertEquals(0, s3Client.list("directory_1/")
      .size());

    Files.deleteIfExists(tempFilePath);
  }

  @ParameterizedTest
  @CsvSource({ "true," + SMALL_SIZE, "true," + LARGE_SIZE, "false," + SMALL_SIZE, "false," + LARGE_SIZE })
  void testAppendFile(boolean isAwsSdk, int size) throws IOException {
    s3Client = S3ClientFactory.getS3Client(getS3ClientProperties(isAwsSdk, port));
    byte[] content = getRandomBytes(size);
    var source = "directory_1/CSV_Data_1.csv";

    // Append to non-existing source
    s3Client.append(source, new ByteArrayInputStream(content));

    // Append to existing source
    s3Client.append(source, new ByteArrayInputStream(content));

    try (var is = s3Client.read(source)) {
      assertTrue(Objects.deepEquals(ArrayUtils.addAll(content, content), is.readAllBytes()));
    }

    s3Client.remove(source);
    System.out.println();

  }

  @DisplayName("Files operations on non-existing file")
  @ParameterizedTest
  @ValueSource(booleans = { true, false })
  void testNonExistingFileOperations(boolean isAwsSdk) {
    s3Client = S3ClientFactory.getS3Client(getS3ClientProperties(isAwsSdk, port));
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

  public static S3ClientProperties getS3ClientProperties(boolean isAwsSdk, int port) {
    return S3ClientProperties.builder()
            .endpoint(format("http://localhost:%d", port))
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
