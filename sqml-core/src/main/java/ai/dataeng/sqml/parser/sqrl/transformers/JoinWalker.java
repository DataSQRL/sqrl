//package ai.dataeng.sqml.parser.sqrl.transformers;
//
//import static ai.dataeng.sqml.parser.sqrl.AliasUtil.getTableAlias;
//import static ai.dataeng.sqml.parser.sqrl.AliasUtil.toIdentifier;
//import static ai.dataeng.sqml.parser.sqrl.analyzer.StatementAnalyzer.walkJoin;
//import static ai.dataeng.sqml.util.SqrlNodeUtil.and;
//
//import ai.dataeng.sqml.parser.AliasGenerator;
//import ai.dataeng.sqml.parser.Column;
//import ai.dataeng.sqml.parser.Relationship;
//import ai.dataeng.sqml.parser.Table;
//import ai.dataeng.sqml.parser.sqrl.LogicalDag;
//import ai.dataeng.sqml.parser.sqrl.analyzer.Scope;
//import ai.dataeng.sqml.parser.sqrl.analyzer.TableBookkeeping;
//import ai.dataeng.sqml.tree.AliasedRelation;
//import ai.dataeng.sqml.tree.ComparisonExpression;
//import ai.dataeng.sqml.tree.ComparisonExpression.Operator;
//import ai.dataeng.sqml.tree.Expression;
//import ai.dataeng.sqml.tree.Identifier;
//import ai.dataeng.sqml.tree.Join;
//import ai.dataeng.sqml.tree.Join.Type;
//import ai.dataeng.sqml.tree.JoinCriteria;
//import ai.dataeng.sqml.tree.JoinOn;
//import ai.dataeng.sqml.tree.Query;
//import ai.dataeng.sqml.tree.Relation;
//import ai.dataeng.sqml.tree.TableNode;
//import ai.dataeng.sqml.tree.TableSubquery;
//import ai.dataeng.sqml.tree.name.Name;
//import ai.dataeng.sqml.tree.name.NamePath;
//import java.util.ArrayList;
//import java.util.List;
//import java.util.Optional;
//import java.util.stream.Collectors;
//import lombok.Value;
//
///**
// * Expands table paths to joins. Maintains a structure
// * in which the rhs is always a table node to make the
// * query easier to debug as it produces more normal
// * looking sql.
// *
// * Retains the user's aliases.
// *
// * Establishes primary keys.
// */
//public class JoinWalker {
//
//  /**
//   * Reminder: Table nodes are only every visited on the furthest
//   * lhs of the relation tree. Otherwise the join node is called.
//   *
//   * TableNodes transform to produce a join tree like:
//   *     /\
//   *    /\ d
//   *   /\
//   *  /\ c
//   * a  b
//   *
//   * The primary key is the set of all of unique primary keys.
//   *
//   * A user defined alias can only be defined on the last table in the
//   * sequence.
//   *
//   * Each name in the table path may need further expanding. The
//   * relationship node will determine how the join should occur.
//   *
//   * The first name will always be either a SELF or a base table.
//   */
//  public WalkResult walkTable(TableNode tableNode, Scope scope, LogicalDag dag) {
//    NamePath namePath = tableNode.getNamePath();
//    Table table = (namePath.getFirst().equals(Name.SELF_IDENTIFIER)) ?
//        scope.getContextTable().get() :
//        dag.getSchema().getByName(namePath.getFirst()).get();
//
//    //Special case: Self identifier is added to join scope
//    if (namePath.getFirst().equals(Name.SELF_IDENTIFIER)) {
//      scope.getJoinScope().put(Name.SELF_IDENTIFIER, scope.getContextTable().get());
//    }
//
//    Name currentAlias;
//    if (namePath.getFirst().equals(Name.SELF_IDENTIFIER)) {
//      currentAlias = Name.SELF_IDENTIFIER;
//    } else {
//      currentAlias = getTableAlias(tableNode, 0);
//    }
//    Relation relation = new TableNode(Optional.empty(), table.getId().toNamePath(), Optional.of(currentAlias));
//    TableBookkeeping b = new TableBookkeeping(relation, currentAlias, table);
//    TableBookkeeping result = walkJoin(tableNode, b, 1);
//
//    return new WalkResult(result.getAlias(), result.getAlias(), result.getCurrentTable(),
//        result.getCurrent());
//  }
//
//  /**
//   */
//  public static WalkResult walkJoin() {
//
//    TableNode rhs = (TableNode)node.getRight();
//    //A join traversal, e.g. FROM orders AS o JOIN o.entries
//    if (scope.getJoinScope().containsKey(rhs.getNamePath().getFirst())) {
//      Name joinAlias = rhs.getNamePath().getFirst();
//      Table table = scope.getJoinScope(joinAlias).get();
//
//      Relationship firstRel = (Relationship)table.getField(rhs.getNamePath().get(1));
//      Name firstAlias = getTableAlias(rhs, 1);
//
//      TableNode relation = new TableNode(Optional.empty(),
//          firstRel.getToTable().getId().toNamePath(),
//          Optional.of(firstAlias));
//
//      TableBookkeeping b = new TableBookkeeping(relation, firstAlias, firstRel.getToTable());
//      TableBookkeeping result = walkJoin(rhs, b, 2);
//
//      scope.getJoinScope().put(result.getAlias(), result.getCurrentTable());
//
//      JoinOn criteria = (JoinOn) getCriteria(firstRel, joinAlias, result.getAlias()).get();
//
//      //Add criteria to join
//      if (node.getCriteria().isPresent()) {
//        List<Expression> newNodes = new ArrayList<>();
//        newNodes.add(rewriteExpression(((JoinOn)node.getCriteria().get()).getExpression(), scope));
//        newNodes.add(criteria.getExpression());
//        criteria = new JoinOn(Optional.empty(), and(newNodes));
//      }
//
//      return new Join(node.getLocation(), node.getType(), (Relation) left.getNode(), (Relation) result.getCurrent(), Optional.of(criteria));
//    } else { //A regular join: FROM Orders JOIN entries
//      Table table = analyzer.getDag().getSchema().getByName(rhs.getNamePath().get(0)).get();
//      Name joinAlias = getTableAlias(rhs, 0);
//      TableNode tableNode = new TableNode(Optional.empty(), table.getId().toNamePath(), Optional.of(joinAlias));
//
//      TableBookkeeping b = new TableBookkeeping(tableNode, joinAlias, table);
//      //Remaining
//      TableBookkeeping result = walkJoin(rhs, b, 1);
//
//      scope.getJoinScope().put(result.getAlias(), result.getCurrentTable());
//
//      Optional<JoinCriteria> criteria = node.getCriteria();
//      if (node.getCriteria().isPresent()) {
//        criteria = Optional.of(new JoinOn(Optional.empty(),
//            rewriteExpression(((JoinOn)node.getCriteria().get()).getExpression(), scope)));
//      }
//
//      return createScope(
//          new Join(node.getLocation(), node.getType(), (Relation) left.getNode(), (Relation) result.getCurrent(), criteria),
//          scope);
//    }
//    return null;
//  }
//
//  /**
//   *
//   */
//  public static WalkResult walkField() {
//    return null;
//  }
//
//
//  public static TableBookkeeping walkJoin(TableNode node, TableBookkeeping b, int startAt) {
//    for (int i = startAt; i < node.getNamePath().getLength(); i++) {
//      Relationship rel = (Relationship)b.getCurrentTable().getField(node.getNamePath().get(i));
//      Name alias = getTableAlias(node, i);
//      Join join = new Join(Optional.empty(), Type.INNER, b.getCurrent(),
//          expandRelation(rel, alias), getCriteria(rel, b.getAlias(), alias));
//      b = new TableBookkeeping(join, alias, rel.getToTable());
//    }
//    return b;
//  }
//
//  public static Relation expandRelation(Relationship rel, Name nextAlias) {
//    if (rel.getType() == Relationship.Type.JOIN) {
//      return new AliasedRelation(Optional.empty(), new TableSubquery(Optional.empty(), (Query)rel.getNode()),
//          new Identifier(Optional.empty(), nextAlias.toNamePath()));
//    } else {
//      return new TableNode(Optional.empty(), rel.getToTable().getId().toNamePath(),
//          Optional.of(nextAlias));
//    }
//  }
//
//  public static class TableObject {
//    List<Name> columnNames;
//  }
//
//  public static class TableSchemaObject extends TableObject {
//    Table table;
//  }
//
//  public static class SubqueryObject extends TableObject {
//    Query query;
//  }
//public static AliasGenerator gen = new AliasGenerator();
//  /**
//   * Walking a path from the join scope table to another table though a relationship
//   */
//  public static Relation getCriteria(Scope scope, Type type, Relation current, Name joinName,
//      TableObject tableObject, Relationship rel, Optional<Name> aliasOpt) {
//
//    Name nextAlias = aliasOpt.orElseGet(()-> gen.nextTableAliasName());
//
//    TableObject tableObject1 = scope.getJoinScope2(joinName);
//
//    Relation result = rel.walk(scope, joinName, current, nextAlias);
//
//    Join join = new Join(Optional.empty(), type, current, null, Optional.empty());
//
//
//  }
//  /**
//   * Create criteria joining on a table, usually for joining on a table that has been broken
//   * out.
//   */
//  public static Optional<JoinCriteria> getCriteria(Name lAlias, Table lhs, Name rAlias, Relation rhs) {
//
//    return null;
//  }
//
//  public static Optional<JoinCriteria> getCriteria(List<Column> columns, Name alias, Name nextAlias) {
//    List<Expression> expr = columns.stream()
//        .map(c -> new ComparisonExpression(Optional.empty(), Operator.EQUAL,
//            toIdentifier(c, alias), toIdentifier(c, nextAlias)))
//        .collect(Collectors.toList());
//    return Optional.of(new JoinOn(Optional.empty(), and(expr)));
//  }
//
//  public static Optional<JoinCriteria> getCriteria(Relationship rel, Name alias, Name nextAlias) {
//    if (rel.type == Relationship.Type.JOIN){
//      //Join expansion uses the context table as its join
//      //these can be renamed, just use pk1, pk2, etc for now
//      List<Column> columns = rel.getTable().getPrimaryKeys();
//      List<Expression> expr = new ArrayList<>();
//      for (int i = 0; i < columns.size(); i++) {
//        Column c = columns.get(i);
//        ComparisonExpression comparisonExpression = new ComparisonExpression(Optional.empty(),
//            Operator.EQUAL,
//            toIdentifier(c, alias),
//            new Identifier(Optional.empty(),
//                nextAlias.toNamePath().concat(Name.system("_pk" + i).toNamePath()))
//        );
//        expr.add(comparisonExpression);
//      }
//      return Optional.of(new JoinOn(Optional.empty(), and(expr)));
//    } else if (rel.type == Relationship.Type.PARENT) {
//      List<Column> columns = rel.getToTable().getPrimaryKeys();
//      return getCriteria(columns, alias, nextAlias);
//    } else {
//      List<Column> columns = rel.getTable().getPrimaryKeys();
//      return getCriteria(columns, alias, nextAlias);
//    }
//  }
//
//
//  @Value
//  public static class WalkResult {
//    Name firstAlias;
//    Name lastAlias;
//    Table lastTable;
//    Relation relation;
//  }
//}
