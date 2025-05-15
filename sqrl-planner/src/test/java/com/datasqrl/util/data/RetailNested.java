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
package com.datasqrl.util.data;

import com.datasqrl.util.TestDataset;
import com.datasqrl.util.TestScript;
import java.nio.file.Path;
import java.util.Set;
import lombok.Getter;

public class RetailNested implements TestDataset {

  public static final Path BASE_PATH = Path.of("..", "..", "sqrl-examples", "retail-nested");

  public static final RetailNested INSTANCE = new RetailNested();

  @Getter public final TestScript testScript;

  public RetailNested() {
    testScript =
        TestScript.of(this, BASE_PATH.resolve("nested.sqrl"), "orders", "productcount").build();
  }

  @Override
  public String getName() {
    return "nested-data";
  }

  @Override
  public Path getDataDirectory() {
    return BASE_PATH.resolve("data");
  }

  @Override
  public Set<String> getTables() {
    return Set.of("orders");
  }

  @Override
  public Path getRootPackageDirectory() {
    return BASE_PATH;
  }

  @Override
  public String toString() {
    return getName();
  }
}
