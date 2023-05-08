package org.folio.s3.client;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.*;
import java.nio.file.Files;

public class OptimizedFileWriter extends StringWriter {

    private final File tmp;
    private final String path;
    private final BufferedWriter writer;
    private final FolioS3Client s3Client;

    public OptimizedFileWriter(String path, int size, FolioS3Client s3Client) {
        try {
            this.s3Client = s3Client;
            this.path = path;

            tmp = Files.createTempFile(FilenameUtils.getName(path), FilenameUtils.getExtension(path)).toFile();

            writer = new BufferedWriter(new FileWriter(tmp), size);
        } catch (IOException e) {
            throw new RuntimeException("Files buffer cannot be created due to error: ", e);
        }
    }

    @Override
    public void write(String data) {
        if (StringUtils.isNotEmpty(data)) {
            try {
                writer.append(data);
            } catch (IOException e) {
                try {
                    Files.deleteIfExists(tmp.toPath());
                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                }
            }
        } else {
            try {
                Files.deleteIfExists(tmp.toPath());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void close() {
        try {
            if (tmp.exists()) {
                writer.close();
                s3Client.write(path, FileUtils.openInputStream(tmp));
            }
        } catch (Exception e) {
            // Just skip and wait file deletion
        } finally {
            try {
                Files.deleteIfExists(tmp.toPath());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}