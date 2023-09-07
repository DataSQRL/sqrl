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

import lombok.Getter;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.Litmus;

import java.util.Optional;

@Getter
public class SqrlAssignTimestamp extends SqrlStatement {

  private final SqlIdentifier path;
  private final SqlNode timestamp;
  private final Optional<SqlIdentifier> alias;
  private final Optional<SqlIdentifier> timestampAlias;

  public SqrlAssignTimestamp(SqlParserPos location,
                             SqlIdentifier path,
                             Optional<SqlIdentifier> alias,
                             SqlNode timestamp,
                             Optional<SqlIdentifier> timestampAlias) {
    super(location, path, Optional.empty());
    this.path = path;
    this.timestamp = timestamp;
    this.alias = alias;
    this.timestampAlias = timestampAlias;
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

  @Override
  public <R, C> R accept(SqrlStatementVisitor<R, C> visitor, C context) {
    return visitor.visit(this, context);
  }

  public SqlNode getAsExpression() {

    return getTimestampAlias()
        .map(a-> (SqlNode)SqlStdOperatorTable.AS.createCall(SqlParserPos.ZERO, getTimestamp(), a))
        .orElse(getTimestamp());
  }
}
