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
import ai.dataeng.sqml.tree.QualifiedName;
import ai.dataeng.sqml.type.Type;
import java.util.Locale;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class Field {
  private Optional<QualifiedObjectName> originTable;
  private Optional<String> originColumnName;
  private Optional<String> relationAlias;
  private Optional<String> name;
  private Type type;
  private boolean hidden;
  private boolean aliased;
  private Optional<QualifiedName> fullPath;
  private Optional<String> originalName;

  public Field(Type type) {

    this.type = type;
  }

  public static Field newUnqualified(String name, Type type)
  {
    requireNonNull(name, "name is null");
    requireNonNull(type, "type is null");

    return new Field(Optional.empty(), Optional.of(name), type, false, Optional.empty(), Optional.empty(), false,
        Optional.empty());
  }

  public static Field newUnqualified(Optional<String> name, Type type)
  {
    requireNonNull(name, "name is null");
    requireNonNull(type, "type is null");

    return new Field(Optional.empty(), name, type, false, Optional.empty(), Optional.empty(), false,
        Optional.empty());
  }

  public static Field newUnqualified(Optional<String> name, Type type, Optional<QualifiedObjectName> originTable, Optional<String> originColumn, boolean aliased)
  {
    requireNonNull(name, "name is null");
    requireNonNull(type, "type is null");
    requireNonNull(originTable, "originTable is null");

    return new Field(Optional.empty(), name, type, false, originTable, originColumn, aliased,
        Optional.empty());
  }

  public static Field newQualified(String relationAlias, Optional<String> name, Type type, boolean hidden, Optional<QualifiedObjectName> originTable, Optional<String> originColumn, boolean aliased)
  {
    requireNonNull(relationAlias, "relationAlias is null");
    requireNonNull(name, "name is null");
    requireNonNull(type, "type is null");
    requireNonNull(originTable, "originTable is null");

    return new Field(Optional.of(relationAlias), name, type, hidden, originTable, originColumn, aliased,
        Optional.empty());
  }


  public static Field newUnqualified(String name, Type type, QualifiedName fullPath) {
      requireNonNull(name, "name is null");
      requireNonNull(type, "type is null");
      requireNonNull(fullPath, "fullPath is null");

      return new Field(Optional.empty(), Optional.of(name), type, false, Optional.empty(), Optional.empty(), false,
          Optional.of(fullPath));
  }
  public static Field newUnqualified(String name, Type type, boolean hidden) {
    requireNonNull(name, "name is null");
    requireNonNull(type, "type is null");

    return new Field(Optional.empty(), Optional.of(name), type, hidden, Optional.empty(), Optional.empty(), false,
        Optional.empty());
  }

  public static Field newDataField(String name, Type type) {
    return new Field(Optional.empty(), Optional.of(name), type, false, Optional.empty(), Optional.empty(), false,
        Optional.empty());
  }

  public Field(Optional<String> relationAlias, Optional<String> name, Type type, boolean hidden,
      Optional<QualifiedObjectName> originTable, Optional<String> originColumnName, boolean aliased,
      Optional<QualifiedName> fullPath)
  {
    requireNonNull(relationAlias, "relationAlias is null");
    requireNonNull(name, "name is null");
    requireNonNull(type, "type is null");
    requireNonNull(originTable, "originTable is null");
    requireNonNull(originColumnName, "originColumnName is null");

    this.relationAlias = relationAlias;
    this.originalName = name;
    this.name = name.map(e->e.toLowerCase(Locale.ROOT));
    this.type = type;
    this.hidden = hidden;
    this.originTable = originTable;
    this.originColumnName = originColumnName;
    this.aliased = aliased;
    this.fullPath = fullPath;
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

  public Type getType() {
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

  public Optional<QualifiedName> getFullPath() {
    return fullPath;
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
