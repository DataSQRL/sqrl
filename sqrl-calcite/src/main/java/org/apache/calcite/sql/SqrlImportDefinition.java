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

import java.util.Optional;
import lombok.Getter;
import org.apache.calcite.sql.parser.SqlParserPos;

@Getter
public class SqrlImportDefinition extends SqrlStatement {

  private final Optional<SqlIdentifier> alias;
  private final SqlIdentifier importPath;

  public SqrlImportDefinition(SqlParserPos location, SqlIdentifier importPath,
      Optional<SqlIdentifier> alias) {
    super(location, importPath, Optional.empty());
    this.alias = alias;
    this.importPath = importPath;
  }

  @Override
  public SqrlImportDefinition clone(SqlParserPos sqlParserPos) {
    return new SqrlImportDefinition(sqlParserPos, importPath, alias);
  }

  public SqrlImportDefinition clone(SqlIdentifier importPath, Optional<SqlIdentifier> alias) {
    return new SqrlImportDefinition(this.pos, importPath, alias);
  }

  @Override
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visit(this, context);
  }
}
