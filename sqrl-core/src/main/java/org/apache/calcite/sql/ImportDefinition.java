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

import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import java.util.Optional;
import lombok.Getter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.Litmus;

@Getter
public class ImportDefinition extends SqrlStatement {

  private final Optional<Name> alias;
  private final Optional<SqlNode> timestamp;
  private final NamePath importPath;

  public ImportDefinition(SqlParserPos location,
      NamePath importPath, Optional<Name> alias, Optional<SqlNode> timestamp) {
    super(location, createNamePath(importPath, alias, timestamp));
    this.alias = alias;
    this.timestamp = timestamp;
    this.importPath = importPath;
  }

  private static NamePath createNamePath(NamePath importPath, Optional<Name> alias,
      Optional<SqlNode> timestamp) {
    NamePath first = alias.map(a->a.toNamePath()).orElse(importPath.popFirst());
    return timestamp.map(ts -> first.concat(getTimestampColumnName(ts))).orElse(first);
  }

  private static Name getTimestampColumnName(SqlNode timestamp) {
    SqlCall call = (SqlCall) timestamp;
    SqlIdentifier columnName = (SqlIdentifier) call.getOperandList().get(1);
    return Name.system(columnName.names.get(0));
  }

  @Override
  public SqlNode clone(SqlParserPos sqlParserPos) {
    return null;
  }

  @Override
  public void unparse(SqlWriter sqlWriter, int i, int i1) {

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
