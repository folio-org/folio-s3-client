package org.folio.s3.client;

import io.minio.http.Method;

import java.io.InputStream;
import java.util.List;
import java.util.concurrent.TimeUnit;

public interface FolioS3Client {
  /**
   * Upload file on S3-compatible storage
   *
   * @param path     the path to the file on S3-compatible storage
   * @param filename path to uploaded file
   * @return the path to the file
   */
  String upload(String path, String filename);

  /**
   * Appends content of input stream to the file on S3 storage. In case file
   * doesn't exist it will be created automatically.
   *
   * @param path the path to the file on S3-compatible storage
   * @param is   input stream with appendable data
   * @return path of updated file
   */
  String append(String path, InputStream is);

  /**
   * Writes bytes to a file on S3-compatible storage
   *
   * @param path the path to the file on S3-compatible storage
   * @param is   the byte array with the bytes to write
   * @return the path to the file
   */
  String write(String path, InputStream is);

  /**
   * Writes Input Stream to a file on S3-compatible storage
   *
   * @param path the path to the file on S3-compatible storage
   * @param is   the byte array with the bytes to write
   * @return the path to the file
   */
  String write(String path, InputStream is, long size);

  /**
   * Removes a file on S3 storage
   *
   * @param path the path to the file to delete
   * @return path of removed file
   */
  String remove(String path);

  /**
   * Removes a files on S3 storage
   *
   * @param paths array of file paths to delete
   * @return list of deleted file paths
   */
  List<String> remove(String... paths);

  /**
   * Opens a file on remote storage, returns an input stream to read from the
   * file. InputStream should be read and closed properly.
   *
   * @param path - the path to the file on S3-compatible storage
   * @return a new input stream with file content
   */
  InputStream read(String path);

  /**
   * Get list of object paths
   *
   * @param path - the path to the file on S3-compatible storage
   * @return list of object paths
   */
  List<String> list(String path);

  /**
   * Get list of object paths, recursively
   *
   * @param path - the path to the file on S3-compatible storage
   * @return list of object paths
   */
  List<String> listRecursive(String path);

  /**
   * Get iterable list of object paths
   *
   * @param path - the path to the file on S3-compatible storage
   * @return iterable list of object paths
   */
  List<String> list(String path, int maxKeys, String startAfter);

  /**
   * Returns size of the object on S3 storage
   *
   * @param path - the path to the file on S3-compatible storage
   * @return size of the object
   */
  long getSize(String path);

  /**
   * Returns RemoteStorageWriter of the S3Client
   *
   * @param path - the path to the temp file that will be used for FileWriter
   * @param size - output-buffer size of FileWriter
   * @return RemoteStorageWriter instance
   */
  RemoteStorageWriter getRemoteStorageWriter(String path, int size);

  /**
   * Returns presigned GET url for object on S3-compatible storage
   *
   * @param path - the path to the file on S3-compatible storage
   * @return presigned url of object
   */
  String getPresignedUrl(String path);

  /**
   * Returns presigned url for object on S3-compatible storage
   *
   * @param path   - the path to the file on S3-compatible storage
   * @param method - http method
   * @return presigned url of object
   */
  String getPresignedUrl(String path, Method method);

  /**
   * Returns presigned url for object on S3-compatible storage with custom expiry
   *
   * @param path       - the path to the file on S3-compatible storage
   * @param method     - http method
   * @param expiryTime - expiry time
   * @param expiryUnit - expiry time unit
   * @return presigned url of object
   */
  String getPresignedUrl(String path, Method method, int expiryTime, TimeUnit expiryUnit);

  /**
   * Creates bucket. Bucket name should be declared in {@link S3ClientProperties}
   */
  void createBucketIfNotExists();

  /**
   * Initiates a multipart upload, returning the upload ID.
   *
   * @param path - the path to the file on S3-compatible storage
   * @return the multipart upload ID
   * @see <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateMultipartUpload.html">
   *     https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateMultipartUpload.html</a>
   */
  String initiateMultipartUpload(String path);

  /**
   * Gets a presigned URL to PUT a part of a multipart upload
   *
   * @param path       - the path to the file on S3-compatible storage
   * @param uploadId   - the upload ID from
   *                   {@link #initiateMultipartUpload(String)}
   * @param partNumber - the part number of the part to upload, starts at 1
   * @return the presigned URL
   * @see #initiateMultipartUpload(String)
   */
  String getPresignedMultipartUploadUrl(
      String path,
      String uploadId,
      int partNumber);

  /**
   * Uploads a part of a multipart upload from a local file
   *
   * @param path       - the path to the file on S3-compatible storage
   * @param uploadId   - the upload ID from
   *                   {@link #initiateMultipartUpload(String)}
   * @param partNumber - the part number of the part to upload, starts at 1
   * @param filename   - the local uploaded file on disk
   * @return the upload's eTag
   * @see #initiateMultipartUpload(String)
   */
  String uploadMultipartPart(
      String path,
      String uploadId,
      int partNumber,
      String filename);

  /**
   * Aborts a multipart upload. Note: **this may need to be done multiple times**
   *
   * @param path     - the path to the file on S3-compatible storage
   * @param uploadId - the upload ID from {@link #initiateMultipartUpload(String)}
   * @see <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_AbortMultipartUpload.html">
   *     https://docs.aws.amazon.com/AmazonS3/latest/API/API_AbortMultipartUpload.html</a>
   * @see #initiateMultipartUpload(String)
   */
  void abortMultipartUpload(String path, String uploadId);

  /**
   * Completes a multipart upload. Note: **this may take several minutes to
   * complete**
   *
   * @param path      - the path to the file on S3-compatible storage
   * @param uploadId  - the upload ID from
   *                  {@link #initiateMultipartUpload(String)}
   * @param partETags - the list of uploaded parts' eTags
   * @see <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html">
   *   https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html</a>
   * @see #initiateMultipartUpload(String)
   * @see #getPresignedMultipartUploadUrl(String, String, int)
   */
  void completeMultipartUpload(
      String path,
      String uploadId,
      List<String> partETags);
}
