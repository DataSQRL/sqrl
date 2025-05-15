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
