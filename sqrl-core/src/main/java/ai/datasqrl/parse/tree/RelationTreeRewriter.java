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

import com.google.common.collect.Iterables;
import java.util.Iterator;
import java.util.Optional;

public final class RelationTreeRewriter<C> {

  private final RelationRewriter<C> rewriter;
  private final AstVisitor<Relation, RelationTreeRewriter.Context<C>> visitor;

  public RelationTreeRewriter(RelationRewriter<C> rewriter) {
    this.rewriter = rewriter;
    this.visitor = new RewritingVisitor();
  }

  public static <C, T extends Relation> T rewriteWith(RelationRewriter<C> rewriter, T node) {
    return new RelationTreeRewriter<>(rewriter).rewrite(node, null);
  }

  public static <C, T extends Relation> T rewriteWith(RelationRewriter<C> rewriter, T node,
      C context) {
    return new RelationTreeRewriter<>(rewriter).rewrite(node, context);
  }

  private static <T> boolean sameElements(Optional<T> a, Optional<T> b) {
    if (!a.isPresent() && !b.isPresent()) {
      return true;
    } else if (a.isPresent() != b.isPresent()) {
      return false;
    }

    return a.get() == b.get();
  }

  @SuppressWarnings("ObjectEquality")
  private static <T> boolean sameElements(Iterable<? extends T> a, Iterable<? extends T> b) {
    if (Iterables.size(a) != Iterables.size(b)) {
      return false;
    }

    Iterator<? extends T> first = a.iterator();
    Iterator<? extends T> second = b.iterator();

    while (first.hasNext() && second.hasNext()) {
      if (first.next() != second.next()) {
        return false;
      }
    }

    return true;
  }

  @SuppressWarnings("unchecked")
  public <T extends Relation> T rewrite(T node, C context) {
    return (T) visitor.process(node, new Context<>(context, false));
  }

  /**
   * Invoke the default rewrite logic explicitly. Specifically, it skips the invocation of the
   * expression rewriter for the provided node.
   */
  @SuppressWarnings("unchecked")
  public <T extends Relation> T defaultRewrite(T node, C context) {
    return (T) visitor.process(node, new Context<>(context, true));
  }

  public static class Context<C> {

    private final boolean defaultRewrite;
    private final C context;

    private Context(C context, boolean defaultRewrite) {
      this.context = context;
      this.defaultRewrite = defaultRewrite;
    }

    public C get() {
      return context;
    }

    public boolean isDefaultRewrite() {
      return defaultRewrite;
    }
  }

  private class RewritingVisitor
      extends AstVisitor<Relation, RelationTreeRewriter.Context<C>> {


    @Override
    public Relation visitGroupingOperation(GroupingOperation node, Context<C> context) {
//      if (!context.isDefaultRewrite()) {
//        Expression result = rewriter
//            .rewriteGroupingOperation(node, context.get(), RelationTreeRewriter.this);
//        if (result != null) {
//          return result;
//        }
//      }
//
//      return node;
      return null;
    }
  }
}
