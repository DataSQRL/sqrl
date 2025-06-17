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

import com.datasqrl.canonicalizer.Name;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;

public interface RelDataTypeBuilder {

  public default RelDataTypeBuilder add(Name name, RelDataType type) {
    return add(name.getDisplay(), type);
  }

  public RelDataTypeBuilder add(String name, RelDataType type);

  public default RelDataTypeBuilder add(Name name, RelDataType type, boolean nullable) {
    return add(name.getDisplay(), type, nullable);
  }

  public RelDataTypeBuilder add(String name, RelDataType type, boolean nullable);

  public RelDataTypeBuilder add(RelDataTypeField field);

  public default RelDataTypeBuilder addAll(Iterable<RelDataTypeField> fields) {
    for (RelDataTypeField field : fields) {
      add(field);
    }
    return this;
  }

  public int getFieldCount();

  public RelDataType build();
}
