package org.folio.s3.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import java.util.Map;

class PutObjectAdditionalOptionsTest {

  @Test
  void testNullGivesNoHeaders() {
    Map<String, String> headers = PutObjectAdditionalOptions.toMinioHeaders(null);

    assertTrue(headers.isEmpty());
  }

  @Test
  void testEmptyGivesNoHeaders() {
    Map<String, String> headers = PutObjectAdditionalOptions.toMinioHeaders(PutObjectAdditionalOptions.builder()
      .build());

    assertTrue(headers.isEmpty());
  }

  @Test
  void testContentHeaders() {
    Map<String, String> headers = PutObjectAdditionalOptions.toMinioHeaders(PutObjectAdditionalOptions.builder()
      .contentDisposition("disposition")
      .contentType("type")
      .build());

    assertEquals(2, headers.size());
    assertEquals("disposition", headers.get("Content-Disposition"));
    assertEquals("type", headers.get("Content-Type"));
  }
}
