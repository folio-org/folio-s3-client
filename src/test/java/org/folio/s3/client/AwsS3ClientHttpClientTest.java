package org.folio.s3.client;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Verifies that {@link AwsS3Client#createS3Client} correctly handles the
 * connection-pool properties from {@link S3ClientProperties}.
 *
 * <p>The AWS SDK's {@link software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient}
 * does not expose its configuration after construction, so these tests focus on
 * verifying that the client is created successfully for each combination rather
 * than inspecting the internal Netty settings via reflection.
 */
class AwsS3ClientHttpClientTest {

  private static final String ENDPOINT = "http://localhost:9000";

  @Test
  @DisplayName("S3AsyncClient is created when no pool properties are set (default Netty config)")
  void defaultNettyConfigWhenNoPropertiesSet() {
    var props = baseProps().build();

    var client = AwsS3Client.createS3Client(props);

    assertNotNull(client, "S3AsyncClient must be created with default Netty configuration");
    client.close();
  }

  @Test
  @DisplayName("S3AsyncClient is created when only idleKeepAliveSeconds is set")
  void nettyConfigWithIdleKeepAliveOnly() {
    var props = baseProps().idleKeepAliveSeconds(10).build();

    var client = AwsS3Client.createS3Client(props);

    assertNotNull(client);
    client.close();
  }

  @Test
  @DisplayName("S3AsyncClient is created when only maxRequestsPerHost is set")
  void nettyConfigWithMaxConcurrencyOnly() {
    var props = baseProps().maxRequestsPerHost(50).build();

    var client = AwsS3Client.createS3Client(props);

    assertNotNull(client);
    client.close();
  }

  @Test
  @DisplayName("S3AsyncClient is created when both idleKeepAliveSeconds and maxRequestsPerHost are set")
  void nettyConfigWithBothPropertiesSet() {
    var props = baseProps().idleKeepAliveSeconds(10).maxRequestsPerHost(50).build();

    var client = AwsS3Client.createS3Client(props);

    assertNotNull(client);
    client.close();
  }

  private static S3ClientProperties.S3ClientPropertiesBuilder baseProps() {
    return S3ClientProperties.builder()
        .endpoint(ENDPOINT)
        .region("us-east-1")
        .bucket("test-bucket")
        .accessKey("ak")
        .secretKey("sk")
        .forcePathStyle(true);
  }
}
