package ai.datasqrl.transform.transforms;

import ai.datasqrl.parse.tree.Relation;
import ai.datasqrl.parse.tree.TableNode;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.schema.Table;
import ai.datasqrl.transform.transforms.JoinWalker.WalkResult;
import ai.datasqrl.transform.visitors.ScopedRelationVisitor;
import ai.datasqrl.validate.paths.RelativeTablePath;
import ai.datasqrl.validate.scopes.StatementScope;
import ai.datasqrl.validate.scopes.TableScope;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class TablePathToJoin extends ScopedRelationVisitor {

  public TablePathToJoin(StatementScope scope) {
    super(scope);
  }

  public Relation rewriteTableNode(TableNode tableNode, TableScope tableScope) {
    NamePath namePath = tableNode.getNamePath();
    Name tableNodeAlias = tableScope.getAlias();

    if (tableNode.getNamePath().getFirst().equals(Name.SELF_IDENTIFIER)) {
      Table table = ((RelativeTablePath) tableScope.getTablePath()).getTable();
      Map<Name, Table> joinScope = new HashMap<>();
      joinScope.put(Name.SELF_IDENTIFIER, table);
      WalkResult result = new JoinWalker().walk(Name.SELF_IDENTIFIER,
          Optional.of(tableNodeAlias),
          namePath.popFirst(),
          Optional.empty(),
          Optional.empty(),
          joinScope);
      return result.getRelation();
    }
    return null;
//    else { //Table is in the schema
//      Table table = analyzer.getDag().getSchema().getByName(namePath.getFirst()).get();
//      scope.getFieldScope().put(tableNodeAlias, table.walk(namePath.popFirst()).get());
//
//      Name firstAlias = AliasUtil.getTableAlias(tableNode, 0, gen::nextTableAliasName);
//      scope.getJoinScope().put(firstAlias, table);
//
//      WalkResult result = new JoinWalker().walk(firstAlias,
//          Optional.of(tableNodeAlias),
//          namePath.popFirst(),
//          Optional.empty(),
//          Optional.empty(),
//          scope.getJoinScope());
//
//      return result.getRelation();
//    }
  }
}
