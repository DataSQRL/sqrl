/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.util;

import com.datasqrl.io.DataSystemDiscoveryConfig;
import com.datasqrl.io.impl.file.DirectoryDataSystemConfig;
import com.datasqrl.util.data.Nutshop;
import com.datasqrl.util.data.Retail;
import com.datasqrl.util.junit.ArgumentProvider;
import java.nio.file.Files;
import lombok.SneakyThrows;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;

import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

public interface TestDataset {

  String getName();

  Path getDataDirectory();

  default DataSystemDiscoveryConfig getDiscoveryConfig() {
    return DirectoryDataSystemConfig.Discovery.builder()
            .directoryURI(getDataDirectory().toUri().getPath())
            .build();
  }

  Set<String> getTables();

  default int getNumTables() {
    return getTables().size();
  }

  default Path getRootPackageDirectory() {
    return getDataDirectory();
  }

    /*
    === STATIC METHODS ===
     */

  static List<TestDataset> getAll() {
    return List.of(Retail.INSTANCE, Nutshop.INSTANCE);
  }

  class AllProvider implements ArgumentsProvider {

    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext extensionContext)
        throws Exception {
      return ArgumentProvider.of(getAll());
    }
  }

  @SneakyThrows
  public static TestDataset ofSingleFile(Path file) {
    TestDataset dataset = new TestDataset() {
      @Override
      public String getName() {
        return "package";
      }

      @Override
      public Path getDataDirectory() {
        return file.getParent();
      }

      @Override
      public Set<String> getTables() {
        String filename = file.getFileName().toString();
        String tableName = filename.substring(0,filename.indexOf('.'));
        return Set.of(tableName);
      }

      public DataSystemDiscoveryConfig getDiscoveryConfig() {
        return DirectoryDataSystemConfig.Discovery.builder()
            .fileURIs(List.of(file.toAbsolutePath().toString()))
            .build();
      }
    };
    Files.createDirectories(dataset.getRootPackageDirectory().resolve(dataset.getName()));
    return dataset;
  }


}
