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

import javax.annotation.Nullable;

/**
 * Concrete elements delegate to its closest abstract parent
 */
public abstract class AstVisitor<R, C> {

  public R process(Node node) {
    return process(node, null);
  }

  public R process(Node node, @Nullable C context) {
    return node.accept(this, context);
  }

  public R visitNode(Node node, C context) {
    return null;
  }

  //SQRL specific nodes

  public R visitScript(ScriptNode node, C context) {
    return visitNode(node, context);
  }

  public R visitImportDefinition(ImportDefinition node, C context) {
    return visitNode(node, context);
  }

  public R visitExportDefinition(ExportDefinition node, C context) {
    return visitNode(node, context);
  }

  public R visitAssignment(Assignment node, C context) {
    return visitNode(node, context);
  }

  public R visitDeclaration(Declaration declaration, C context) {
    return visitNode(declaration, context);
  }

  public R visitExpressionAssignment(ExpressionAssignment node, C context) {
    return visitAssignment(node, context);
  }

  public R visitQueryAssignment(QueryAssignment node, C context) {
    return visitAssignment(node, context);
  }

  public R visitCreateSubscription(CreateSubscription node, C context) {
    return visitNode(node, context);
  }

  public R visitDistinctAssignment(DistinctAssignment node, C context) {
    return visitAssignment(node, context);
  }

  public R visitJoinAssignment(JoinAssignment node, C context) {
    return visitAssignment(node, context);
  }

  public R visitStatement(Statement node, C context) {
    return visitNode(node, context);
  }

  public R visitSelectItem(SelectItem node, C context) {
    return visitNode(node, context);
  }

  public R visitSingleColumn(SingleColumn node, C context) {
    return visitSelectItem(node, context);
  }

  public R visitAllColumns(AllColumns node, C context) {
    return visitSelectItem(node, context);
  }

  public R visitIdentifier(Identifier node, C context) {
    return visitNode(node, context);
  }
}
