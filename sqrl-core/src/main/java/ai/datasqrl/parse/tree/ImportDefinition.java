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
package ai.datasqrl.parse.tree;

import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class ImportDefinition extends SqrlStatement {

  protected final NodeLocation location;
  protected final NamePath namePath;
  private final Optional<Identifier> alias;
  private final Optional<SingleColumn> timestamp;

  public ImportDefinition(NodeLocation location,
      NamePath namePath, Optional<Identifier> alias, Optional<SingleColumn> timestamp) {
    super(Optional.ofNullable(location));
    this.location = location;
    this.namePath = namePath;
    this.alias = alias;
    this.timestamp = timestamp;
  }

  public NamePath getNamePath() {
    return namePath;
  }

  public Optional<Identifier> getAlias() {
    return alias;
  }

  public Optional<Name> getAliasName() {
    return getAlias().map(a -> a.getNamePath().getFirst());
  }

  public Optional<SingleColumn> getTimestamp() {
    return timestamp;
  }

  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitImportDefinition(this, context);
  }

  public List<? extends Node> getChildren() {
    return null;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ImportDefinition anImport = (ImportDefinition) o;
    return Objects.equals(location, anImport.location) && Objects
        .equals(namePath, anImport.namePath);
  }

  @Override
  public int hashCode() {
    return Objects.hash(location, namePath);
  }

  @Override
  public String toString() {
    return "Import{" +
        "location=" + location +
        ", namePath=" + namePath +
        '}';
  }
}
