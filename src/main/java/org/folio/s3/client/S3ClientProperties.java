package org.folio.s3.client;

import lombok.Builder;
import lombok.Data;

/**
 * Configuration properties for configuring the S3 client.
 *
 * <p>This class contains essential connection settings such as endpoint, region, credentials, and
 * bucket name. It is used to initialize {@link AwsS3Client} or other S3-compatible clients.
 */
@Data
@Builder
public class S3ClientProperties {

  /** URL to object storage. */
  private String endpoint;

  /** The region to configure the url. */
  private String region;

  /** The object storage bucket. */
  private String bucket;

  /** The object storage subfolder. */
  private String subPath;

  /** The credentials for access to object storage - accessKey. */
  private String accessKey;

  /** The credentials for access to object storage - secretKey. */
  private String secretKey;

  /** Key that enables files merging in storage with using AWS SDK capabilities. */
  private boolean awsSdk;

  /** True for bucket name in the path, false for bucket name in the virtual host name. */
  private boolean forcePathStyle;
}
