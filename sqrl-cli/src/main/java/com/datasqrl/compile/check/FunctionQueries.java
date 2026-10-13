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
package com.datasqrl.compile.check;

import com.datasqrl.compile.check.PassthroughReferences.Table;
import com.datasqrl.compile.check.SymbolIndex.Column;
import com.datasqrl.error.ErrorLocation.FileLocation;
import com.datasqrl.planner.parser.SqrlPassthroughTableFunctionStatement;
import com.datasqrl.planner.parser.SqrlTableFunctionStatement;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import lombok.experimental.UtilityClass;
import org.apache.flink.table.catalog.ObjectIdentifier;

/**
 * Finds the names in the queries of table functions that the planner does not resolve: the SQL of
 * pass-through functions, which the planner does not validate, and the columns of the parent table
 * that a relationship names with {@code this.}, which the parser replaces with parameters.
 */
@UtilityClass
class FunctionQueries {

  /**
   * A name that refers to a table or column, with locations relative to its statement.
   *
   * @param start where the name starts
   * @param end where its last character is
   * @param table the table
   * @param column the column and its nested fields, empty for the table
   * @param type the type of the column
   */
  record Resolved(
      FileLocation start,
      FileLocation end,
      ObjectIdentifier table,
      List<String> column,
      Optional<String> type) {}

  /**
   * The names in the SQL of a pass-through function, as it is written in the statement, with the
   * parameters of the function.
   *
   * @param statement the text of the statement
   * @param tables the tables that the SQL can name
   * @param parent the table of a relationship
   */
  static List<Resolved> passthrough(
      String statement,
      SqrlPassthroughTableFunctionStatement function,
      List<Table> tables,
      Optional<Table> parent) {
    var source = new ScriptText(statement);
    var body = function.getDefinitionBody().getFileLocation();
    var bodyStart = source.offset(body.line() - 1, body.offset() - 1);
    var result = new ArrayList<Resolved>();
    var sql = source.text().substring(bodyStart);
    for (var found : PassthroughReferences.resolve(sql, tables, parent)) {
      result.add(
          new Resolved(
              location(source, bodyStart + found.start()),
              location(source, bodyStart + found.end() - 1),
              found.table(),
              found.column(),
              found.type()));
    }
    return result;
  }

  /** The columns of the parent table that a relationship names with {@code this.}. */
  static List<Resolved> parentColumns(SqrlTableFunctionStatement relationship, Table parent) {
    var result = new ArrayList<Resolved>();
    for (var argument : relationship.getArgumentsByIndex()) {
      var name = argument.getName();
      var column =
          argument.isParentField() ? parent.column(List.of(name.get())) : Optional.<Column>empty();
      if (column.isPresent()) {
        var start = name.getFileLocation();
        result.add(
            new Resolved(
                start,
                new FileLocation(start.line(), start.offset() + name.get().length() - 1),
                parent.identifier(),
                List.of(column.get().name()),
                Optional.of(column.get().type())));
      }
    }
    return result;
  }

  /** The 1-based location of an offset, as the parser locates names. */
  private static FileLocation location(ScriptText text, int offset) {
    return new FileLocation(text.lineOf(offset) + 1, text.characterOf(offset) + 1);
  }
}
