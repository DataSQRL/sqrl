/*
 * Copyright © 2021 DataSQRL (contact@datasqrl.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
    var p = FileUtil.makeHiddenFolder(Path.of("./"), "datasqrl-test");
    assertTrue(Files.isDirectory(p));
    Files.deleteIfExists(p);
  }

  @Test
  public void testFileName() {
    assertEquals("file.txt", FileUtil.getFileName("/../../my/folder/file.txt"));
    assertEquals("file.txt", FileUtil.getFileName("file.txt"));
  }
}
