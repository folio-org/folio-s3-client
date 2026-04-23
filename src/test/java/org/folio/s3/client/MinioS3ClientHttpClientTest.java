package org.folio.s3.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.lang.reflect.Field;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import io.minio.MinioAsyncClient;
import okhttp3.OkHttpClient;

/**
 * Unit tests for the OkHttp connection-pool wiring driven by
 * {@link S3ClientProperties#getIdleKeepAliveSeconds()} in {@link MinioS3Client#createClient}.
 *
 * <p>Uses reflection to inspect the {@link OkHttpClient} instance held by the underlying
 * {@link MinioAsyncClient}, since neither minio-java nor OkHttp expose it publicly.
 */
class MinioS3ClientHttpClientTest {

  private static final String ENDPOINT = "http://localhost:9000";

  @Test
  @DisplayName("Default OkHttp keep-alive (5 minutes) is used when idleKeepAliveSeconds is not set")
  void defaultKeepAliveWhenPropertyNotSet() throws Exception {
    var props = baseProps().build();

    var client = MinioS3Client.createClient(props);
    var okHttp = extractOkHttpClient(client);

    assertNotNull(okHttp, "Minio client must always have an OkHttpClient");
    // OkHttp default ConnectionPool keep-alive is 5 minutes
    assertEquals(TimeUnit.MINUTES.toNanos(5), keepAliveNanos(okHttp),
        "Default OkHttp keep-alive should be 5 minutes when property is null");
  }

  @Test
  @DisplayName("Custom OkHttp keep-alive is applied when idleKeepAliveSeconds is set")
  void customKeepAliveWhenPropertySet() throws Exception {
    var props = baseProps().idleKeepAliveSeconds(15).build();

    var client = MinioS3Client.createClient(props);
    var okHttp = extractOkHttpClient(client);

    assertNotNull(okHttp);
    assertEquals(TimeUnit.SECONDS.toNanos(15), keepAliveNanos(okHttp),
        "OkHttp keep-alive must match the configured idleKeepAliveSeconds");
  }

  /**
   * OkHttp 5 doesn't expose the keep-alive duration on the public {@link okhttp3.ConnectionPool}
   * API anymore, but {@code getDelegate$okhttp()} returns the {@code RealConnectionPool} which
   * does expose {@code getKeepAliveDurationNs$okhttp()} (both are JVM-public despite the suffix).
   */
  private static long keepAliveNanos(OkHttpClient okHttp) throws Exception {
    var pool = okHttp.connectionPool();
    var delegate = pool.getClass().getMethod("getDelegate$okhttp").invoke(pool);
    return (long) delegate.getClass().getMethod("getKeepAliveDurationNs$okhttp").invoke(delegate);
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

  /**
   * Pulls the private {@code httpClient} field from {@link MinioAsyncClient}'s class hierarchy.
   * The field is declared on {@code io.minio.S3Base} (parent of MinioAsyncClient).
   */
  private static OkHttpClient extractOkHttpClient(MinioAsyncClient client) throws IllegalAccessException {
    Class<?> c = client.getClass();
    while (c != null) {
      for (Field f : c.getDeclaredFields()) {
        if (OkHttpClient.class.isAssignableFrom(f.getType())) {
          f.setAccessible(true);
          return (OkHttpClient) f.get(client);
        }
      }
      c = c.getSuperclass();
    }
    throw new IllegalStateException("OkHttpClient field not found on MinioAsyncClient hierarchy");
  }
}

