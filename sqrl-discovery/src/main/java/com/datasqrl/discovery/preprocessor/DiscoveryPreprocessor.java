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
package com.datasqrl.discovery.preprocessor;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.packager.preprocessor.Preprocessor;
import java.nio.file.Files;
import java.nio.file.Path;

public interface DiscoveryPreprocessor extends Preprocessor {

  String DISABLE_DISCOVERY_FILENAME = ".nodiscovery";

  @Override
  default void processFile(Path file, ProcessorContext processorContext, ErrorCollector errors) {
    if (!runDiscovery(file.getParent())) {
      return;
    }
    discoverFile(file, processorContext, errors);
  }

  void discoverFile(Path file, ProcessorContext processorContext, ErrorCollector errors);

  /**
   * Checks whether the directory contains a file that indicates discovery is not enabled in this
   * directory.
   *
   * @return
   */
  private static boolean runDiscovery(Path directory) {
    // Safety check: check if the directory exists and is a directory
    if (directory == null || !Files.exists(directory) || !Files.isDirectory(directory)) {
      return false;
    }

    // Check if the directory contains a file named .nodiscovery
    var noDiscoveryFilePath = directory.resolve(DISABLE_DISCOVERY_FILENAME);
    return !Files.exists(noDiscoveryFilePath) || Files.isDirectory(noDiscoveryFilePath);
  }
}
