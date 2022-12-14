package org.folio.s3.client;

import java.io.InputStream;
import java.util.List;

public interface FolioS3Client {

    /**
     * Upload file on S3-compatible storage
     *
     * @param path the path to the file on S3-compatible storage
     * @param filename path to uploaded file
     * @return the path to the file
     */
    String upload(String path, String filename);

    /**
     * Appends content of input stream to the file on S3 storage. In case file doesn't exist it will be created automatically.
     * @param path the path to the file on S3-compatible storage
     * @param is input stream with appendable data
     * @return path of updated file
     */
    String append(String path, InputStream is);
    /**
     * Writes bytes to a file on S3-compatible storage
     *
     * @param path the path to the file on S3-compatible storage
     * @param is the byte array with the bytes to write
     * @return the path to the file
     */
    String write(String path, InputStream is);

    /**
     * Removes a file on S3 storage
     *
     * @param path the path to the file to delete
     * @return path of removed file
     */
    String remove(String path);

    /**
     * Removes a files on S3 storage
     * @param paths array of file paths to delete
     * @return list of deleted file paths
     */
    List<String> remove(String... paths);

    /**
     * Opens a file on remote storage, returns an input stream to read from the file.
     * InputStream should be read and closed properly.
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
     * Returns size of the object on S3 storage
     *
     * @param path - the path to the file on S3-compatible storage
     * @return size of the object
     */
    long getSize(String path);
}
