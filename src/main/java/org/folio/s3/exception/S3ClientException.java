package org.folio.s3.exception;

public class S3ClientException extends RuntimeException {

    public S3ClientException(String message) {
        super(message);
    }

    public S3ClientException(String message, Exception e) {
        super(message, e);
    }

    public S3ClientException(Exception e) {
        super(e);
    }
}
