package org.folio.s3.client;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.file.Files;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.folio.s3.exception.S3ClientException;

public class RemoteStorageWriter extends StringWriter {

  private final File tmp;
  private final String path;
  private final BufferedWriter writer;
  private final FolioS3Client s3Client;

  public RemoteStorageWriter(String path, int size, FolioS3Client s3Client) {
    try {
      this.s3Client = s3Client;
      this.path = path;

      this.tmp = Files.createTempFile(FilenameUtils.getName(path), FilenameUtils.getExtension(path))
        .toFile();

      this.writer = new BufferedWriter(new FileWriter(this.tmp), size);
    } catch (Exception ex) {
      throw new S3ClientException("Files buffer cannot be created due to error: " + ex.getMessage());
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