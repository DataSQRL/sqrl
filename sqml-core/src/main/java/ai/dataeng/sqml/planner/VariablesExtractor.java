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
package ai.dataeng.sqml.planner;


import static java.util.Objects.requireNonNull;

import ai.dataeng.sqml.relation.VariableReferenceExpression;
import ai.dataeng.sqml.sql.tree.DefaultExpressionTraversalVisitor;
import ai.dataeng.sqml.sql.tree.DefaultTraversalVisitor;
import ai.dataeng.sqml.sql.tree.DereferenceExpression;
import ai.dataeng.sqml.sql.tree.Expression;
import ai.dataeng.sqml.sql.tree.Identifier;
import ai.dataeng.sqml.sql.tree.NodeRef;
import ai.dataeng.sqml.sql.tree.QualifiedName;
import ai.dataeng.sqml.sql.tree.SymbolReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableSet;
import java.util.List;
import java.util.Set;

public final class VariablesExtractor {

  private VariablesExtractor() {
  }

  @Deprecated
  public static Set<VariableReferenceExpression> extractUnique(
      Iterable<? extends Expression> expressions, TypeProvider types) {
    ImmutableSet.Builder<VariableReferenceExpression> unique = ImmutableSet.builder();
    for (Expression expression : expressions) {
      unique.addAll(extractAll(expression, types));
    }
    return unique.build();
  }

  public static List<VariableReferenceExpression> extractAll(Expression expression,
      TypeProvider types) {
    ImmutableList.Builder<VariableReferenceExpression> builder = ImmutableList.builder();
    new VariableFromExpressionBuilderVisitor(types).process(expression, builder);
    return builder.build();
  }

  // to extract qualified name with prefix
  public static Set<QualifiedName> extractNames(Expression expression,
      Set<NodeRef<Expression>> columnReferences) {
    ImmutableSet.Builder<QualifiedName> builder = ImmutableSet.builder();
    new QualifiedNameBuilderVisitor(columnReferences).process(expression, builder);
    return builder.build();
  }

  private static class VariableFromExpressionBuilderVisitor
      extends DefaultExpressionTraversalVisitor<Void, Builder<VariableReferenceExpression>> {

    private final TypeProvider types;

    protected VariableFromExpressionBuilderVisitor(TypeProvider types) {
      this.types = types;
    }

    @Override
    protected Void visitSymbolReference(SymbolReference node,
        ImmutableList.Builder<VariableReferenceExpression> builder) {
      builder.add(new VariableReferenceExpression(node.getName(), types.get(node)));
      return null;
    }
  }

  private static class QualifiedNameBuilderVisitor
      extends DefaultTraversalVisitor<Void, ImmutableSet.Builder<QualifiedName>> {

    private final Set<NodeRef<Expression>> columnReferences;

    private QualifiedNameBuilderVisitor(Set<NodeRef<Expression>> columnReferences) {
      this.columnReferences = requireNonNull(columnReferences, "columnReferences is null");
    }

    @Override
    protected Void visitDereferenceExpression(DereferenceExpression node,
        ImmutableSet.Builder<QualifiedName> builder) {
      if (columnReferences.contains(NodeRef.<Expression>of(node))) {
        builder.add(DereferenceExpression.getQualifiedName(node));
      } else {
        process(node.getBase(), builder);
      }
      return null;
    }

    @Override
    protected Void visitIdentifier(Identifier node, ImmutableSet.Builder<QualifiedName> builder) {
      builder.add(QualifiedName.of(node.getValue()));
      return null;
    }
  }
}
