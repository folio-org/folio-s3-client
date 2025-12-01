package org.folio.s3.client;

import static org.apache.commons.io.FilenameUtils.getExtension;
import static org.apache.commons.io.FilenameUtils.getName;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.folio.s3.exception.S3ClientException;

/**
 * Writer implementation that buffers data into a temporary local file and uploads it to remote
 * storage via {@link FolioS3Client} when closed.
 *
 * <p>The content written to this writer is not kept in memory, but streamed into a temporary file
 * to avoid excessive memory usage for large payloads. On {@link #close()}, the file is sent to the
 * configured S3-compatible storage and then deleted from the local filesystem.
 */
public class RemoteStorageWriter extends StringWriter {

  private static final Path SAFE_TMP_DIR;

  static {
    try {
      SAFE_TMP_DIR = Files.createTempDirectory("folio-s3-");
      SAFE_TMP_DIR.toFile().setReadable(true, true);
      SAFE_TMP_DIR.toFile().setWritable(true, true);
      SAFE_TMP_DIR.toFile().setExecutable(true, true);
    } catch (IOException e) {
      throw new IllegalStateException("Cannot create safe temp directory", e);
    }
  }

  private final File tmp;
  private final String path;
  private final BufferedWriter writer;
  private final FolioS3Client s3Client;

  /**
   * Creates a new {@code RemoteStorageWriter} instance that writes data to a temporary file and
   * uploads it to remote storage on close.
   *
   * @param path key or path under which the file will be stored in remote storage
   * @param size buffer size for the underlying {@link BufferedWriter}
   * @param s3Client client used to upload the temporary file to S3-compatible storage
   * @throws S3ClientException if the temporary file or writer cannot be created
   */
  public RemoteStorageWriter(String path, int size, FolioS3Client s3Client) {
    try {
      this.s3Client = s3Client;
      this.path = path;

      this.tmp =
          Files.createTempFile(SAFE_TMP_DIR, getName(path), "." + getExtension(path)).toFile();

      this.writer = new BufferedWriter(new FileWriter(this.tmp), size);
    } catch (Exception ex) {
      throw new S3ClientException(
          "Files buffer cannot be created due to error: " + ex.getMessage());
    }
  }

  @Override
  public void write(String data) {
    if (StringUtils.isNotEmpty(data)) {
      try {
        writer.append(data);
      } catch (IOException e) {
        deleteTmp(tmp);
      }
    } else {
      deleteTmp(tmp);
    }
  }

  @Override
  public void close() {
    try {
      if (tmp.exists()) {
        writer.close();
        s3Client.write(path, FileUtils.openInputStream(tmp));
      }
    } catch (Exception ex) {
      throw new S3ClientException("Error while close(): " + ex.getMessage());
    } finally {
      deleteTmp(tmp);
    }
  }

  private void deleteTmp(File tmp) {
    try {
      Files.deleteIfExists(tmp.toPath());
    } catch (IOException ex) {
      throw new S3ClientException("Error in deleting file: " + ex.getMessage());
    }
  }
}
