package ai.datasqrl.schema.join;

/**
 * Inline-able join declaration:
 *  X.y := JOIN Z ON true;
 *  X.a := SELECT * FROM _ JOIN _.y;
 * Primary keys from the context relation can be substituted to eliminate the join.
 * This allows joins to be interpolated without needing to do a duplicate join.
 * e.g.
 * SELECT * FROM X x0 JOIN (X x1 JOIN Z z1 ON true) ON x0.pk = x1.pk;
 * ->
 * SELECT * FROM X JOIN Z ON true;
 * Top-N join declaration:
 *  X.y := JOIN Z ON true LIMIT 5;
 *  X.a := SELECT * FROM _ JOIN _.y;
 * ->
 * SELECT * FROM X x0 JOIN (SELECT x.pk AS _pk1, z.* FROM X JOIN Z LIMIT 5) x ON x._pk1 = x0.pk
 * The order of the relation matters for Top-N but not for inlined joins.
 */
public interface JoinDeclarationVisitor<R, C> {

  R visitInline(InlineJoinDeclaration inlineJoinDeclaration, C context);
  R visitTopN(TopNJoinDeclaration topNJoinDeclaration, C context);
}
