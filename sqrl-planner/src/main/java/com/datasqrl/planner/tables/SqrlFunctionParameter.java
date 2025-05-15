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
package com.datasqrl.planner.tables;

import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.Value;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.FunctionParameter;

/** A parameter of {@link SqrlTableFunction} */
@Value
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
@ToString(onlyExplicitlyIncluded = true)
public class SqrlFunctionParameter implements FunctionParameter {

  @EqualsAndHashCode.Include @ToString.Include
  String name; // the properly resolved name of the argument

  int ordinal; // the index within the list of query arguments

  @EqualsAndHashCode.Include @ToString.Include
  RelDataType relDataType; // this is the type of the argument

  boolean
      isParentField; // if true, this is a column on the "this" table, else a user provided argument

  @Override
  public RelDataType getType(RelDataTypeFactory relDataTypeFactory) {
    return relDataType;
  }

  @Override
  public boolean isOptional() {
    return false;
  }
}
