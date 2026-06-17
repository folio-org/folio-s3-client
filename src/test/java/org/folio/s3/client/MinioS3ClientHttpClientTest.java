package org.folio.s3.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.lang.reflect.Field;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import io.minio.MinioAsyncClient;
import okhttp3.Dispatcher;
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

  @Test
  @DisplayName("Custom maxIdleConnections is applied to the OkHttp connection pool")
  void customMaxIdleConnectionsWhenPropertySet() throws Exception {
    var props = baseProps().idleKeepAliveSeconds(10).maxIdleConnections(30).build();

    var client = MinioS3Client.createClient(props);
    var okHttp = extractOkHttpClient(client);

    assertNotNull(okHttp);
    assertEquals(30, maxIdleConnections(okHttp),
        "OkHttp connection pool max idle must match the configured maxIdleConnections");
  }

  @Test
  @DisplayName("Default maxIdleConnections (5) is used when only idleKeepAliveSeconds is set")
  void defaultMaxIdleConnectionsWhenNotSet() throws Exception {
    var props = baseProps().idleKeepAliveSeconds(10).build();

    var client = MinioS3Client.createClient(props);
    var okHttp = extractOkHttpClient(client);

    assertNotNull(okHttp);
    assertEquals(5, maxIdleConnections(okHttp),
        "Default OkHttp maxIdleConnections should be 5 when maxIdleConnections is null");
  }

  @Test
  @DisplayName("Custom maxRequestsPerHost is applied to the OkHttp Dispatcher")
  void customMaxRequestsPerHostWhenPropertySet() throws Exception {
    var props = baseProps().idleKeepAliveSeconds(10).maxRequestsPerHost(50).build();

    var client = MinioS3Client.createClient(props);
    var okHttp = extractOkHttpClient(client);

    assertNotNull(okHttp);
    assertEquals(50, okHttp.dispatcher().getMaxRequestsPerHost(),
        "OkHttp dispatcher maxRequestsPerHost must match the configured value");
  }

  @Test
  @DisplayName("Default maxRequestsPerHost (5) is used when only idleKeepAliveSeconds is set")
  void defaultMaxRequestsPerHostWhenNotSet() throws Exception {
    var props = baseProps().idleKeepAliveSeconds(10).build();

    var client = MinioS3Client.createClient(props);
    var okHttp = extractOkHttpClient(client);

    assertNotNull(okHttp);
    assertEquals(new Dispatcher().getMaxRequestsPerHost(), okHttp.dispatcher().getMaxRequestsPerHost(),
        "Default OkHttp maxRequestsPerHost should match Dispatcher's built-in default when not configured");
  }

  /**
   * OkHttp 5 doesn't expose the keep-alive duration on the public {@link okhttp3.ConnectionPool}
   * API anymore, but {@code getDelegate$okhttp()} returns the {@code RealConnectionPool} which
   * does expose {@code getKeepAliveDurationNs$okhttp()} and {@code getMaxIdleConnections$okhttp()}
   * (both are JVM-public despite the suffix).
   */
  private static long keepAliveNanos(OkHttpClient okHttp) throws Exception {
    var pool = okHttp.connectionPool();
    var delegate = pool.getClass().getMethod("getDelegate$okhttp").invoke(pool);
    return (long) delegate.getClass().getMethod("getKeepAliveDurationNs$okhttp").invoke(delegate);
  }

  /**
   * {@code RealConnectionPool.maxIdleConnections} is private with no public getter, so we read it
   * via field reflection.
   */
  private static int maxIdleConnections(OkHttpClient okHttp) throws Exception {
    var pool = okHttp.connectionPool();
    var delegate = pool.getClass().getMethod("getDelegate$okhttp").invoke(pool);
    var field = delegate.getClass().getDeclaredField("maxIdleConnections");
    field.setAccessible(true);
    return (int) field.get(delegate);
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

