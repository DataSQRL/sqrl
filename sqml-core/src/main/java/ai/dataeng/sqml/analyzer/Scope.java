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

import static ai.dataeng.sqml.analyzer.SemanticExceptions.ambiguousAttributeException;
import static ai.dataeng.sqml.analyzer.SemanticExceptions.missingAttributeException;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Objects.requireNonNull;

import ai.dataeng.sqml.common.type.UnknownType;
import ai.dataeng.sqml.sql.tree.DereferenceExpression;
import ai.dataeng.sqml.sql.tree.Expression;
import ai.dataeng.sqml.sql.tree.Identifier;
import ai.dataeng.sqml.sql.tree.QualifiedName;
import java.util.List;
import java.util.Optional;
import javax.annotation.concurrent.Immutable;

@Immutable
public class Scope {

  private final Optional<Scope> parent;
  private final boolean queryBoundary;
  private final RelationId relationId;
  private final RelationType relation;

  private Scope(
      Optional<Scope> parent,
      boolean queryBoundary,
      RelationId relationId,
      RelationType relation) {
    this.parent = requireNonNull(parent, "parent is null");
    this.relationId = requireNonNull(relationId, "relationId is null");
    this.queryBoundary = queryBoundary;
    this.relation = requireNonNull(relation, "relation is null");
  }

  public static Scope create() {
    return builder().build();
  }

  public static Builder builder() {
    return new Builder();
  }

  private static QualifiedName asQualifiedName(Expression expression) {
    QualifiedName name = null;
    if (expression instanceof Identifier) {
      name = QualifiedName.of(((Identifier) expression).getValue());
    } else if (expression instanceof DereferenceExpression) {
      name = DereferenceExpression.getQualifiedName((DereferenceExpression) expression);
    }
    return name;
  }

  private static boolean isColumnReference(QualifiedName name, RelationType relation) {
    while (name.getPrefix().isPresent()) {
      name = name.getPrefix().get();
      if (!relation.resolveFields(name).isEmpty()) {
        return true;
      }
    }
    return false;
  }

  public Optional<Scope> getOuterQueryParent() {
    Scope scope = this;
    while (scope.parent.isPresent()) {
      if (scope.queryBoundary) {
        return scope.parent;
      }
      scope = scope.parent.get();
    }

    return Optional.empty();
  }

  public Optional<Scope> getLocalParent() {
    if (!queryBoundary) {
      return parent;
    }

    return Optional.empty();
  }

  public RelationId getRelationId() {
    return relationId;
  }

  public RelationType getRelationType() {
    return relation;
  }

  public ResolvedField resolveField(Expression expression, QualifiedName name) {
    return tryResolveField(expression, name)
        .orElseThrow(() -> missingAttributeException(expression, name));
  }

  public Optional<ResolvedField> tryResolveField(Expression expression) {
    QualifiedName qualifiedName = asQualifiedName(expression);
    if (qualifiedName != null) {
      return tryResolveField(expression, qualifiedName);
    }
    return Optional.empty();
  }

  public Optional<ResolvedField> tryResolveField(Expression node, QualifiedName name) {
    return resolveField(node, name, 0, true);
  }

  private Optional<ResolvedField> resolveField(Expression node, QualifiedName name,
      int fieldIndexOffset, boolean local) {
    List<Field> matches = relation.resolveFields(name);
    if (matches.size() > 1) {
      throw ambiguousAttributeException(node, name);
    } else if (matches.size() == 1) {
      return Optional.of(asResolvedField(getOnlyElement(matches), fieldIndexOffset, local));
    } else {
      if (isColumnReference(name, relation)) {
        return Optional.empty();
      }
      if (parent.isPresent()) {
        return parent.get().resolveField(node, name, fieldIndexOffset + relation.getAllFieldCount(),
            local && !queryBoundary);
      }

      //Todo: resolve parent field
      return Optional.of(
          new ResolvedField(this, Field.newUnqualified(name.toString(), UnknownType.UNKNOWN),
              0, 0, local)
      );
    }
  }

  private ResolvedField asResolvedField(Field field, int fieldIndexOffset, boolean local) {
    int relationFieldIndex = relation.indexOf(field);
    int hierarchyFieldIndex = relation.indexOf(field) + fieldIndexOffset;
    return new ResolvedField(this, field, hierarchyFieldIndex, relationFieldIndex, local);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .addValue(relationId)
        .toString();
  }

  public static final class Builder {

    private RelationId relationId = RelationId.anonymous();
    private RelationType relationType = new RelationType();
    private Optional<Scope> parent = Optional.empty();
    private boolean queryBoundary;

    public Builder withRelationType(RelationId relationId, RelationType relationType) {
      this.relationId = requireNonNull(relationId, "relationId is null");
      this.relationType = requireNonNull(relationType, "relationType is null");
      return this;
    }

    public Builder withParent(Scope parent) {
      checkArgument(!this.parent.isPresent(), "parent is already set");
      this.parent = Optional.of(parent);
      return this;
    }

    public Builder withOuterQueryParent(Scope parent) {
      checkArgument(!this.parent.isPresent(), "parent is already set");
      this.parent = Optional.of(parent);
      this.queryBoundary = true;
      return this;
    }

    public Scope build() {
      return new Scope(parent, queryBoundary, relationId, relationType);
    }
  }
}
