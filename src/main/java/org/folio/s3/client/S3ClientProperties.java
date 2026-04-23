package org.folio.s3.client;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class S3ClientProperties {

  /**
   * URL to object storage.
   */
  private String endpoint;

  /**
   * The region to configure the url.
   */
  private String region;

  /**
   * The object storage bucket.
   */
  private String bucket;

  /**
   * The object storage subfolder.
   */
  private String subPath;

  /**
   * The credentials for access to object storage - accessKey.
   */
  private String accessKey;

  /**
   *  The credentials for access to object storage - secretKey.
   */
  private String secretKey;

  /**
   * Key that enables files merging in storage with using AWS SDK capabilities.
   */
  private boolean awsSdk;

  /**
   * True for bucket name in the path, false for bucket name in the virtual host name.
   */
  private boolean forcePathStyle;

  /**
   * Maximum time, in seconds, that an idle HTTP connection in the OkHttp connection pool
   * (used by the underlying Minio client) is kept alive before being evicted.
   *
   * <p>If {@code null}, the OkHttp default of 5 minutes is used. Set this to a value smaller
   * than the server-side idle timeout (AWS S3 closes idle connections after ~20s) to avoid
   * "unexpected end of stream" / connection-reset errors, especially during multipart uploads
   * — which always go through the Minio client, even when {@link AwsS3Client} is used.
   */
  private Integer idleKeepAliveSeconds;
}
