package org.folio.s3.client;

import java.util.HashMap;
import java.util.Map;

import lombok.Builder;
import lombok.Data;

/**
 * Additional options supported for write and compose operations.
 */
@Data
@Builder
public class PutObjectAdditionalOptions {
  private String contentDisposition;
  private String contentType;

  public static Map<String, String> toMinioHeaders(PutObjectAdditionalOptions options) {
    if (options == null) {
      return Map.of();
    }

    Map<String, String> headers = new HashMap<>();
    if (options.getContentDisposition() != null) {
      headers.put("Content-Disposition", options.getContentDisposition());
    }
    if (options.getContentType() != null) {
      headers.put("Content-Type", options.getContentType());
    }
    return headers;
  }
}
