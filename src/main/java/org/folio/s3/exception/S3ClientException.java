package org.folio.s3.exception;

/**
 * Exception thrown by the FOLIO S3 client when an error occurs while interacting with S3-compatible
 * storage. This runtime exception wraps lower-level I/O or client-specific exceptions to provide a
 * unified error handling mechanism.
 */
public class S3ClientException extends RuntimeException {

  /**
   * Constructs a new {@link S3ClientException} with the specified detail message.
   *
   * @param message the detail message describing the error
   */
  public S3ClientException(String message) {
    super(message);
  }

  /**
   * Constructs a new {@link S3ClientException} with the specified detail message and the underlying
   * cause.
   *
   * @param message the detail message describing the error
   * @param e the cause of this exception
   */
  public S3ClientException(String message, Exception e) {
    super(message, e);
  }
}
