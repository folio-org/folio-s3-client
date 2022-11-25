package org.folio.s3.client;

public class S3ClientFactory {

    /**
     * Returns {@link FolioS3Client} implementation based on value of {@link S3ClientProperties#isAwsSdk()} value
     * @param s3ClientProperties - S3 client properties
     * @return {@link AwsS3Client} if {@link S3ClientProperties#isAwsSdk()} is true, otherwise - {@link MinioS3Client}
     */
    public static FolioS3Client getS3Client(S3ClientProperties s3ClientProperties) {
        if (s3ClientProperties.isAwsSdk()) {
            return new AwsS3Client(s3ClientProperties);
        } else {
            return new MinioS3Client(s3ClientProperties);
        }
    }
}
