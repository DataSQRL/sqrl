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

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;

/**
 * The private directory of a language server for the build output of its checks. The server deletes
 * it when it exits, but a server that is killed leaves it behind, so a server also deletes the
 * directories of servers that no longer run: each server holds a lock on a file in its directory
 * while it runs.
 */
@Slf4j
public final class WorkDirectory {

  static final String PREFIX = "sqrl-lsp-";
  static final String LOCK = ".lock";

  /** A directory without a lock that is newer may still be getting its lock */
  private static final Duration UNLOCKED_GRACE = Duration.ofMinutes(1);

  /** The open lock files of the directories of this process, which hold their locks */
  private static final Map<Path, FileChannel> LOCKS = new ConcurrentHashMap<>();

  private WorkDirectory() {}

  /**
   * Creates the directory of this server in a parent directory, after deleting the directories of
   * servers that no longer run there. The directory is deleted when the process exits.
   *
   * @param parent the parent directory, e.g. the temporary directory of the system
   */
  public static Path create(Path parent) throws IOException {
    deleteAbandoned(parent);
    var dir = Files.createTempDirectory(parent, PREFIX);
    var lock =
        FileChannel.open(dir.resolve(LOCK), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
    lock.lock();
    LOCKS.put(dir, lock);
    Runtime.getRuntime().addShutdownHook(new Thread(() -> release(dir), "sqrl-lsp-cleanup"));
    return dir;
  }

  /** Releases the lock of a directory of this process and deletes it, e.g. when it exits. */
  static void release(Path dir) {
    var lock = LOCKS.remove(dir);
    if (lock != null) {
      try {
        lock.close();
      } catch (IOException e) {
        log.debug("Could not release {}", dir, e);
      }
    }
    delete(dir);
  }

  /** Deletes the directories in a parent directory whose servers no longer run. */
  static void deleteAbandoned(Path parent) {
    try (var dirs = Files.newDirectoryStream(parent, PREFIX + "*")) {
      for (var dir : dirs) {
        if (Files.isDirectory(dir) && isAbandoned(dir)) {
          log.debug("Deleting {} of a language server that no longer runs", dir);
          delete(dir);
        }
      }
    } catch (IOException e) {
      log.debug("Could not list {}", parent, e);
    }
  }

  /** Whether no server holds the lock of a directory. */
  private static boolean isAbandoned(Path dir) {
    if (LOCKS.containsKey(dir)) {
      // Closing another channel of its lock file would release the lock on some systems
      return false;
    }
    var lock = dir.resolve(LOCK);
    if (!Files.exists(lock)) {
      return isOlderThan(dir, UNLOCKED_GRACE);
    }
    try (var channel = FileChannel.open(lock, StandardOpenOption.WRITE)) {
      var acquired = channel.tryLock();
      if (acquired == null) {
        return false; // the lock of another process
      }
      acquired.release();
      return true;
    } catch (OverlappingFileLockException e) {
      return false; // the lock of this process
    } catch (IOException e) {
      return false;
    }
  }

  private static boolean isOlderThan(Path dir, Duration age) {
    try {
      return Files.getLastModifiedTime(dir).toInstant().isBefore(Instant.now().minus(age));
    } catch (IOException e) {
      return false;
    }
  }

  /** Deletes a directory with its content, as far as possible. */
  static void delete(Path dir) {
    if (!Files.exists(dir)) {
      return;
    }
    try (var paths = Files.walk(dir)) {
      paths
          .sorted(Comparator.reverseOrder())
          .forEach(
              path -> {
                try {
                  Files.deleteIfExists(path);
                } catch (IOException e) {
                  log.debug("Could not delete {}", path, e);
                }
              });
    } catch (IOException e) {
      log.debug("Could not delete {}", dir, e);
    }
  }
}
