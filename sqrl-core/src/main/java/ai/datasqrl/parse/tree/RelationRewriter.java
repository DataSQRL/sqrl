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

public class RelationRewriter<C> {

  public Relation rewriteRelation(Relation node, C context,
      RelationTreeRewriter<C> treeRewriter) {
    return null;
  }

  public Relation rewriteAliasedRelation(AliasedRelation node, C context,
      RelationTreeRewriter<C> treeRewriter) {
    return rewriteRelation(node, context, treeRewriter);
  }
  public Relation rewriteExcept(Except node, C context,
      RelationTreeRewriter<C> treeRewriter) {
    return rewriteRelation(node, context, treeRewriter);
  }
  public Relation rewriteIntersect(Intersect node, C context,
      RelationTreeRewriter<C> treeRewriter) {
    return rewriteRelation(node, context, treeRewriter);
  }
  public Relation rewriteJoin(Join node, C context,
      RelationTreeRewriter<C> treeRewriter) {
    return rewriteRelation(node, context, treeRewriter);
  }
  public Relation rewriteQuerySpecification(QuerySpecification node, C context,
      RelationTreeRewriter<C> treeRewriter) {
    return rewriteRelation(node, context, treeRewriter);
  }
  public Relation rewriteTableNode(TableNode node, C context,
      RelationTreeRewriter<C> treeRewriter) {
    return rewriteRelation(node, context, treeRewriter);
  }
  public Relation rewriteTableSubquery(TableSubquery node, C context,
      RelationTreeRewriter<C> treeRewriter) {
    return rewriteRelation(node, context, treeRewriter);
  }
  public Relation rewriteUnion(Union node, C context,
      RelationTreeRewriter<C> treeRewriter) {
    return rewriteRelation(node, context, treeRewriter);
  }
}
