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
import com.google.common.base.Preconditions;
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
  private final Optional<SqlIdentifier> timestampAlias;

  public ImportDefinition(SqlParserPos location,
      NamePath importPath, Optional<Name> alias, Optional<SqlNode> timestamp,
      Optional<SqlIdentifier> timestampAlias) {
    super(location, createNamePath(importPath, alias, timestamp, timestampAlias), Optional.empty());
    this.alias = alias;
    this.timestamp = timestamp;
    this.importPath = importPath;
    this.timestampAlias = timestampAlias;
    if (timestampAlias.isPresent()) {
      Preconditions.checkState(timestamp.isPresent());
    }
  }

  private static NamePath createNamePath(NamePath importPath, Optional<Name> alias,
      Optional<SqlNode> timestamp, Optional<SqlIdentifier> timestampAlias) {
    if (timestamp.isPresent() && timestampAlias.isEmpty()) {
      Preconditions.checkState(timestamp.get() instanceof SqlIdentifier,
          "Timestamp must be an identifier or use the AS keyword to define a new column");
      SqlIdentifier identifier = (SqlIdentifier)timestamp.get();
      Preconditions.checkState(identifier.names.size() == 1, "Identifier should not be nested or contain a path");
      return importPath.popFirst().concat(Name.system(identifier.names.get(0)));
    }

    NamePath first = alias.map(a->a.toNamePath()).orElse(importPath.popFirst());
    return timestampAlias
        .map(ts -> first.concat(Name.system(ts.names.get(0)))).orElse(first);
  }

  @Override
  public SqlNode clone(SqlParserPos sqlParserPos) {
    return null;
  }

  @Override
  public void unparse(SqlWriter sqlWriter, int i, int i1) {
    sqlWriter.keyword("IMPORT");
    sqlWriter.print(this.importPath.getDisplay());
    //todo remaining
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
