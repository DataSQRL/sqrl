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
package com.datasqrl.lsp;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class WorkDirectoryTest {

  @TempDir Path parent;

  @Test
  void givenDirectoryOfServerThatWasKilled_whenServerStarts_thenItIsDeleted() throws IOException {
    // Its lock file is left, but no process holds the lock
    var abandoned = Files.createDirectories(parent.resolve(WorkDirectory.PREFIX + "killed"));
    Files.writeString(abandoned.resolve(WorkDirectory.LOCK), "");
    Files.writeString(abandoned.resolve("build.txt"), "output");

    var dir = WorkDirectory.create(parent);
    try {
      assertThat(abandoned).doesNotExist();
      assertThat(dir.resolve(WorkDirectory.LOCK)).isRegularFile();
    } finally {
      WorkDirectory.release(dir);
    }
  }

  @Test
  void givenDirectoryOfServerThatRuns_whenAnotherServerStarts_thenItIsKept() throws IOException {
    var running = WorkDirectory.create(parent);
    try {
      var other = WorkDirectory.create(parent);
      WorkDirectory.release(other);

      assertThat(running).isDirectory();
      assertThat(other).doesNotExist();
    } finally {
      WorkDirectory.release(running);
    }
  }

  @Test
  void givenDirectoryWithoutLock_whenServerStarts_thenOnlyAnOldOneIsDeleted() throws IOException {
    var old = Files.createDirectories(parent.resolve(WorkDirectory.PREFIX + "old"));
    Files.setLastModifiedTime(old, FileTime.from(Instant.now().minus(Duration.ofHours(1))));
    // E.g. of a server that is just about to take its lock
    var recent = Files.createDirectories(parent.resolve(WorkDirectory.PREFIX + "recent"));

    WorkDirectory.release(WorkDirectory.create(parent));

    assertThat(old).doesNotExist();
    assertThat(recent).isDirectory();
  }
}
