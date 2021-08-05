/*
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
package ai.dataeng.sqml.analyzer;


import ai.dataeng.sqml.OperatorType.QualifiedObjectName;
import ai.dataeng.sqml.tree.Identifier;
import ai.dataeng.sqml.tree.QualifiedName;
import ai.dataeng.sqml.type.SqmlType;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class Field
{
  private final Optional<QualifiedObjectName> originTable;
  private final Optional<String> originColumnName;
  private final Optional<String> relationAlias;
  private final Optional<String> name;
  private final SqmlType type;
  private final boolean hidden;
  private final boolean aliased;

  public static Field newUnqualified(String name, SqmlType type)
  {
    requireNonNull(name, "name is null");
    requireNonNull(type, "type is null");

    return new Field(Optional.empty(), Optional.of(name), type, false, Optional.empty(), Optional.empty(), false);
  }

  public static Field newUnqualified(Optional<String> name, SqmlType type)
  {
    requireNonNull(name, "name is null");
    requireNonNull(type, "type is null");

    return new Field(Optional.empty(), name, type, false, Optional.empty(), Optional.empty(), false);
  }

  public static Field newUnqualified(Optional<String> name, SqmlType type, Optional<QualifiedObjectName> originTable, Optional<String> originColumn, boolean aliased)
  {
    requireNonNull(name, "name is null");
    requireNonNull(type, "type is null");
    requireNonNull(originTable, "originTable is null");

    return new Field(Optional.empty(), name, type, false, originTable, originColumn, aliased);
  }

  public static Field newQualified(String relationAlias, Optional<String> name, SqmlType type, boolean hidden, Optional<QualifiedObjectName> originTable, Optional<String> originColumn, boolean aliased)
  {
    requireNonNull(relationAlias, "relationAlias is null");
    requireNonNull(name, "name is null");
    requireNonNull(type, "type is null");
    requireNonNull(originTable, "originTable is null");

    return new Field(Optional.of(relationAlias), name, type, hidden, originTable, originColumn, aliased);
  }

  public Field(Optional<String> relationAlias, Optional<String> name, SqmlType type, boolean hidden, Optional<QualifiedObjectName> originTable, Optional<String> originColumnName, boolean aliased)
  {
    requireNonNull(relationAlias, "relationAlias is null");
    requireNonNull(name, "name is null");
    requireNonNull(type, "type is null");
    requireNonNull(originTable, "originTable is null");
    requireNonNull(originColumnName, "originColumnName is null");

    this.relationAlias = relationAlias;
    this.name = name;
    this.type = type;
    this.hidden = hidden;
    this.originTable = originTable;
    this.originColumnName = originColumnName;
    this.aliased = aliased;
  }

  public Optional<QualifiedObjectName> getOriginTable() {
    return originTable;
  }

  public Optional<String> getOriginColumnName()
  {
    return originColumnName;
  }

  public Optional<String> getRelationAlias()
  {
    return relationAlias;
  }

  public Optional<String> getName()
  {
    return name;
  }

  public SqmlType getType()
  {
    return type;
  }

  public boolean isHidden()
  {
    return hidden;
  }

  public boolean isAliased()
  {
    return aliased;
  }

  @Override
  public String toString()
  {
    StringBuilder result = new StringBuilder();
    if (relationAlias.isPresent()) {
      result.append(relationAlias.get())
          .append(".");
    }

    result.append(name.orElse("<anonymous>"))
        .append(":")
        .append(type);

    return result.toString();
  }
}
