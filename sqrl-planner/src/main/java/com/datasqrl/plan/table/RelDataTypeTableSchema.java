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
package com.datasqrl.plan.table;

import com.datasqrl.io.tables.TableSchema;
import java.nio.file.Path;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.calcite.rel.type.RelDataType;

@AllArgsConstructor
@Getter
public class RelDataTypeTableSchema implements TableSchema {
  RelDataType relDataType;

  @Override
  public String getSchemaType() {
    return null;
  }

  @Override
  public String getDefinition() {
    return null;
  }

  @Override
  public Optional<Path> getLocation() {
    return Optional.empty();
  }
}
