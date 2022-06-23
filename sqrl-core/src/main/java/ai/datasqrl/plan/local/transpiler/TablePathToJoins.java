package ai.datasqrl.plan.local.transpiler;

import ai.datasqrl.parse.tree.Expression;
import ai.datasqrl.parse.tree.Hint;
import ai.datasqrl.parse.tree.Join.Type;
import ai.datasqrl.parse.tree.JoinOn;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.parse.tree.name.ReservedName;
import ai.datasqrl.plan.local.transpiler.nodes.relation.JoinNorm;
import ai.datasqrl.plan.local.transpiler.nodes.relation.RelationNorm;
import ai.datasqrl.plan.local.transpiler.nodes.relation.TableNodeNorm;
import ai.datasqrl.plan.local.transpiler.nodes.schemaRef.RelationshipRef;
import ai.datasqrl.plan.local.transpiler.nodes.schemaRef.SelfRef;
import ai.datasqrl.plan.local.transpiler.nodes.schemaRef.TableOrRelationship;
import ai.datasqrl.plan.local.transpiler.nodes.schemaRef.TableRef;
import ai.datasqrl.plan.local.transpiler.util.CriteriaUtil;
import ai.datasqrl.schema.Field;
import ai.datasqrl.schema.Relationship;
import ai.datasqrl.schema.Table;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * This class identifies table paths in the query and expands them to joins.
 *
 * A query with a table path looks like this:
 * SELECT * FROM _.user.posts;
 *
 * It will expand it to a query that looks like this:
 * SELECT * FROM _ JOIN user ON _._uuid = user._uuid JOIN posts ON user.id = posts.user_id;
 */
public class TablePathToJoins {

  public static RelationNorm expand(NamePath namePath, List<Hint> hints, RelationScope scope) {
    List<TableOrRelationship> walk = mapToReference(namePath, scope);
    return expand(walk, hints);
  }

  /**
   * Turn a table path into the references of either table, relationship, or self
   */
  public static List<TableOrRelationship> mapToReference(NamePath namePath, RelationScope scope) {
    if (namePath.getLength() > 1 && namePath.get(0).equals(ReservedName.SELF_IDENTIFIER) &&
        !scope.getHasExpandedSelf().get()) {
      //Special case 2: has a self reference but we have not expanded table yet
      RelationNorm self = scope.getJoinScopes().get(ReservedName.SELF_IDENTIFIER);
      scope.getHasExpandedSelf().set(true);

      List<TableOrRelationship> rels = new ArrayList<>();
      rels.add(new SelfRef(scope.getContextTable().get(), self));
      Optional<List<Field>> remaining = scope.getContextTable().get().walkFields(namePath.popFirst());
      if (remaining.isEmpty()) {
        return rels;
      }
      List<TableOrRelationship> relationships = remaining.get().stream()
          .map(f->new RelationshipRef((Relationship) f))
          .collect(Collectors.toList());
      rels.addAll(relationships);
      return rels;
    } else if (scope.getJoinScopes().get(namePath.getFirst()) != null) {
      RelationNorm rel = scope.getJoinScopes().get(namePath.getFirst());
      Preconditions.checkState(rel instanceof TableNodeNorm, "Table paths to inner queries not supported");
      Table table = ((TableNodeNorm) rel).getRef().getTable();

      Optional<List<Field>> remaining = table.walkFields(namePath.popFirst());
      if (remaining.isEmpty()) {
        throw new RuntimeException("Cannot find path");
      }
      return table.walkFields(namePath.popFirst()).get().stream()
          .map(f->new RelationshipRef((Relationship) f))
          .collect(Collectors.toList());
    } else {
      List<TableOrRelationship> rels = new ArrayList<>();
      Table table = scope.getSchema().getVisibleByName(namePath.getFirst())
          .orElseThrow(()->new RuntimeException(namePath.toString()));
      rels.add(new TableRef(table));
      Optional<List<Field>> remaining = table.walkFields(namePath.popFirst());
      if (remaining.isEmpty()) {
        return rels;
      }
      List<TableOrRelationship> relationships = remaining.get().stream()
          .map(f->new RelationshipRef((Relationship) f))
          .collect(Collectors.toList());
      rels.addAll(relationships);
      return rels;
    }
  }

  public static RelationNorm expand(List<TableOrRelationship> fields, List<Hint> hints) {
    if (fields.size() == 1) {
      return expand(fields.get(0), hints);
    }
    RelationNorm left = expand(fields.get(0), List.of());
    RelationNorm right = expand(fields.subList(1, fields.size()), hints);
    Expression criteria = CriteriaUtil.sameTableEq(left.getRightmost(), right.getLeftmost());

    return new JoinNorm(Optional.empty(), Type.INNER, left, right, JoinOn.on(criteria));
  }

  private static RelationNorm expand(TableOrRelationship field, List<Hint> hints) {
    if (field instanceof SelfRef) {
      return ((SelfRef)field).getSelf();
    } else if (field instanceof TableRef) {
      return new TableNodeNorm(Optional.empty(), field.getTable().getPath(),
          Optional.empty(), field, false, hints);
    } else {
      Relationship rel = ((RelationshipRef) field).getRelationship();
      return rel.getRelation();
    }
  }
}
