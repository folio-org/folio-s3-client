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
}
