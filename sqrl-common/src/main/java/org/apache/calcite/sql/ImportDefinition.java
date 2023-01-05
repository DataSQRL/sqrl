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
package org.apache.calcite.sql;

import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Optional;
import lombok.Getter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.Litmus;

@Getter
public class ImportDefinition extends SqrlStatement {

  private final Optional<SqlIdentifier> alias;
  private final Optional<SqlNode> timestamp;
  private final SqlIdentifier importPath;
  private final Optional<SqlIdentifier> timestampAlias;

  public ImportDefinition(SqlParserPos location,
      SqlIdentifier importPath, Optional<SqlIdentifier> alias, Optional<SqlNode> timestamp,
      Optional<SqlIdentifier> timestampAlias) {
    super(location, transformIdentifier(importPath, alias, timestamp, timestampAlias),
        Optional.empty());
    this.alias = alias;
    this.timestamp = timestamp;
    this.importPath = importPath;
    this.timestampAlias = timestampAlias;
    if (timestampAlias.isPresent()) {
      Preconditions.checkState(timestamp.isPresent());
    }
  }

  /**
   * Rearrange the import path to have a valid assignment location for timestamps.
   */
  private static SqlIdentifier transformIdentifier(SqlIdentifier importPath,
      Optional<SqlIdentifier> alias,
      Optional<SqlNode> timestamp, Optional<SqlIdentifier> timestampAlias) {
    String tableName = alias.map(e -> e.names.get(0)).orElseGet(() ->
        importPath.names.get(importPath.names.size() - 1));
    if (timestamp.isPresent() && timestampAlias.isEmpty()) {
      Preconditions.checkState(timestamp.get() instanceof SqlIdentifier,
          "Timestamp must be an identifier or use the AS keyword to define a new column");
      SqlIdentifier ts = (SqlIdentifier) timestamp.get();
      Preconditions.checkState(ts.names.size() == 1,
          "Identifier should not be nested or contain a path");
      return new SqlIdentifier(List.of(tableName, ts.names.get(0)), ts.getParserPosition());
    }

    SqlIdentifier first = alias
        .orElse(
            new SqlIdentifier(importPath.names.size() == 1
                ? List.of(importPath.names.get(0))
                : importPath.names.subList(0, importPath.names.size() - 1),
            importPath.getParserPosition()));
    return timestampAlias
        //Concat the alias to the end if there's an alias
        .map(ts -> new SqlIdentifier(List.of(tableName, ts.names.get(0)),
            importPath.getParserPosition()))
        .orElse(first);
  }

  @Override
  public SqlNode clone(SqlParserPos sqlParserPos) {
    return null;
  }

  @Override
  public void unparse(SqlWriter sqlWriter, int i, int i1) {
    sqlWriter.keyword("IMPORT");
    importPath.unparse(sqlWriter, i, i1);
  }

  @Override
  public void validate(SqlValidator sqlValidator, SqlValidatorScope sqlValidatorScope) {

  }

  @Override
  public <R> R accept(SqlVisitor<R> sqlVisitor) {
    return null;
  }

  @Override
  public boolean equalsDeep(SqlNode sqlNode, Litmus litmus) {
    return false;
  }
}
