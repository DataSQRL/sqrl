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
package com.datasqrl.planner.analyzer;

import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;

/**
 * A column name written outside of SQL: in a hint, such as {@code primary_key(customerId)}, or as a
 * required column of a tag in {@code compiler.tags}. SQL matches column names case-sensitively,
 * these names match like identifiers in Postgres:
 *
 * <ul>
 *   <li>A plain name, such as {@code customerid}, matches the column with the same name ignoring
 *       case. It is ambiguous when it matches several columns whose names differ only in case.
 *   <li>A name in backticks, such as {@code `customerId`}, only matches the column with exactly
 *       that name. A backtick in the name is doubled, as in Flink SQL.
 * </ul>
 *
 * <p>Unlike Postgres, a plain name is not folded to lower case, since Flink keeps the case of
 * column names.
 *
 * @param name the column name, without the backticks
 * @param quoted whether the name is written in backticks
 */
public record ColumnReference(String name, boolean quoted) {

  private static final Pattern QUOTED = Pattern.compile("`((?:[^`]|``)+)`");

  /** Parses a column name as it is written. A name that is not all in backticks is plain. */
  public static ColumnReference parse(String written) {
    var quoted = QUOTED.matcher(written);
    return quoted.matches()
        ? new ColumnReference(quoted.group(1).replace("``", "`"), true)
        : new ColumnReference(written, false);
  }

  /**
   * The names of the columns in backticks, for the error about an ambiguous name, e.g. {@code `cnt`
   * or `CNT`}.
   */
  public static String alternatives(List<RelDataTypeField> columns) {
    return columns.stream()
        .map(column -> quote(column.getName()))
        .collect(Collectors.joining(" or "));
  }

  /** The columns of the row type that this name matches, in the order of the row type. */
  public List<RelDataTypeField> matchingColumns(RelDataType rowType) {
    return rowType.getFieldList().stream().filter(field -> matches(field.getName())).toList();
  }

  /** Whether this name and the other name would match the same column. */
  public boolean overlaps(ColumnReference other) {
    return quoted && other.quoted ? name.equals(other.name) : name.equalsIgnoreCase(other.name);
  }

  private boolean matches(String columnName) {
    return quoted ? columnName.equals(name) : columnName.equalsIgnoreCase(name);
  }

  /** The name as it is written: in backticks if it is quoted. */
  @Override
  public String toString() {
    return quoted ? quote(name) : name;
  }

  /**
   * The name in backticks, with each backtick in it doubled, so it matches exactly one column, e.g.
   * {@code `CNT`}.
   */
  public static String quote(String name) {
    return "`" + name.replace("`", "``") + "`";
  }
}
