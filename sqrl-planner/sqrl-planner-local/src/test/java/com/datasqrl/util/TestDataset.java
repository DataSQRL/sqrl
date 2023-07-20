/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.util;

import com.datasqrl.io.ExternalDataType;
import com.datasqrl.io.impl.file.FileDataSystemConfig;
import com.datasqrl.io.impl.file.FileDataSystemFactory;
import com.datasqrl.io.tables.TableConfig;
import com.datasqrl.util.data.Nutshop;
import com.datasqrl.util.data.Retail;
import com.datasqrl.util.junit.ArgumentProvider;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;

public interface TestDataset {

  String getName();

  Path getDataDirectory();

  default TableConfig getDiscoveryConfig() {
    return FileDataSystemFactory.getFileDiscoveryConfig(getName(),
            FileDataSystemConfig.builder()
                .directoryURI(getDataDirectory().toString())
                .source$$monitor$interval("0")
                .build())
        .build();
  }

  Set<String> getTables();

  default int getNumTables() {
    return getTables().size();
  }

  default Path getRootPackageDirectory() {
    return getDataDirectory();
  }

  default Path getDataPackageDirectory() {
    return getRootPackageDirectory().resolve(getName());
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


}
