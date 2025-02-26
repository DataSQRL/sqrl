package com.datasqrl.discovery.preprocessor;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.packager.preprocess.Preprocessor;
import java.nio.file.Files;
import java.nio.file.Path;

public interface DiscoveryPreprocessor extends Preprocessor {

  String DISABLE_DISCOVERY_FILENAME = ".nodiscovery";

  @Override
  default void processFile(Path file, ProcessorContext processorContext, ErrorCollector errors) {
    if (!runDiscovery(file.getParent())) return;
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
    if (!Files.exists(directory) || !Files.isDirectory(directory)) {
      return false;
    }

    // Check if the directory contains a file named .nodiscovery
    Path noDiscoveryFilePath = directory.resolve(DISABLE_DISCOVERY_FILENAME);
    return !Files.exists(noDiscoveryFilePath) || Files.isDirectory(noDiscoveryFilePath);
  }
}
