package com.datasqrl.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

public class FileUtilTest {

  @Test
  public void testHiddenFolder() throws IOException {
    Path p = FileUtil.makeHiddenFolder(Path.of("./"), "datasqrl-test");
    assertTrue(Files.isDirectory(p));
    Files.deleteIfExists(p);
  }

  @Test
  public void testFileName() {
    assertEquals("file.txt", FileUtil.getFileName("/../../my/folder/file.txt"));
    assertEquals("file.txt", FileUtil.getFileName("file.txt"));
  }
}
