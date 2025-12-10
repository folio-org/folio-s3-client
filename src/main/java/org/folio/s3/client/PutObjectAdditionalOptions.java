package org.folio.s3.client;

import java.util.HashMap;
import java.util.Map;
import lombok.Builder;
import lombok.Data;

/**
 * Represents additional HTTP header options that can be applied to object write and compose
 * operations in S3-compatible storage.
 *
 * <p>This class allows specifying optional parameters such as {@code Content-Disposition} and
 * {@code Content-Type}, which are converted into MinIO-compatible header formats when performing
 * upload or compose actions.
 */
@Data
@Builder
public class PutObjectAdditionalOptions {
  private String contentDisposition;
  private String contentType;

  /**
   * Converts the provided {@link PutObjectAdditionalOptions} instance into a map of HTTP headers
   * compatible with MinIO client operations.
   *
   * @param options an instance of {@code PutObjectAdditionalOptions}; may be {@code null}
   * @return a map of header key-value pairs; never {@code null}
   */
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
