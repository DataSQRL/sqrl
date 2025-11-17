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

import com.datasqrl.graphql.exec.FlinkExecFunction;
import com.datasqrl.graphql.server.ResolvedMetadata;
import java.util.Optional;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.Value;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.FunctionParameter;

/** A parameter of {@link SqrlTableFunction} */
@Value
@RequiredArgsConstructor
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
@ToString(onlyExplicitlyIncluded = true)
public class SqrlFunctionParameter implements FunctionParameter {

  @EqualsAndHashCode.Include @ToString.Include
  String name; // the properly resolved name of the argument

  String description;

  int ordinal;

  @EqualsAndHashCode.Include @ToString.Include RelDataType relDataType;

  // if true, this is a column on the "this" table, else a user provided argument
  boolean isParentField;

  Optional<ResolvedMetadata> metadata;

  Optional<FlinkExecFunction> function;

  public SqrlFunctionParameter(String name, int ordinal, RelDataType relDataType) {
    this(name, "", ordinal, relDataType, false, Optional.empty(), Optional.empty());
  }

  @Override
  public RelDataType getType(RelDataTypeFactory relDataTypeFactory) {
    return relDataType;
  }

  @Override
  public boolean isOptional() {
    return false;
  }

  public boolean isMetadata() {
    return metadata.isPresent();
  }

  public boolean isFunction() {
    return function.isPresent();
  }

  public boolean isExternalArgument() {
    return !isParentField && metadata.isEmpty() && function.isEmpty();
  }
}
