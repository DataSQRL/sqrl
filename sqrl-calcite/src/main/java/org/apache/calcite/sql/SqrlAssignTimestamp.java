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
}
