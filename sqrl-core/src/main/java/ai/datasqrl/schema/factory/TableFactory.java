package ai.datasqrl.schema.factory;

import static ai.datasqrl.parse.util.SqrlNodeUtil.and;

import ai.datasqrl.parse.tree.ComparisonExpression;
import ai.datasqrl.parse.tree.ComparisonExpression.Operator;
import ai.datasqrl.parse.tree.Expression;
import ai.datasqrl.parse.tree.Join.Type;
import ai.datasqrl.parse.tree.JoinOn;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.plan.local.transpiler.nodes.expression.ResolvedColumn;
import ai.datasqrl.plan.local.transpiler.nodes.relation.JoinNorm;
import ai.datasqrl.plan.local.transpiler.nodes.relation.RelationNorm;
import ai.datasqrl.plan.local.transpiler.nodes.relation.TableNodeNorm;
import ai.datasqrl.schema.Column;
import ai.datasqrl.schema.Field;
import ai.datasqrl.schema.Relationship;
import ai.datasqrl.schema.Relationship.JoinType;
import ai.datasqrl.schema.Relationship.Multiplicity;
import ai.datasqrl.schema.ShadowingContainer;
import ai.datasqrl.schema.Table;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class TableFactory {

  public final static AtomicInteger tableIdCounter = new AtomicInteger(0);

  public Table createTable(NamePath tableName, List<Column> fields) {
    Table table = createTableInternal(tableName, fields);
    return table;
  }

  public void assignRelationships(Name name, Table table, Table parentTable) {
    //Built-in relationships
    Relationship parent = new Relationship(Name.PARENT_RELATIONSHIP,
        table, parentTable, JoinType.PARENT, Multiplicity.ONE,
        createRelation(Type.INNER, parentTable.getPrimaryKeys(), table, parentTable),
        Optional.empty(), Optional.empty());
    table.getFields().add(parent);

    Relationship child = new Relationship(name,
        parentTable, table, JoinType.CHILD, Multiplicity.MANY,
        createRelation(Type.INNER, parentTable.getPrimaryKeys(), parentTable, table),
        Optional.empty(), Optional.empty());
    parentTable.getFields().add(child);
  }

  private Table createTableInternal(NamePath tableName, List<Column> fields) {
    return new Table(tableIdCounter.incrementAndGet(),
        tableName, false, toShadowContainer(fields));
  }

  private ShadowingContainer<Field> toShadowContainer(List<Column> fields) {
    ShadowingContainer<Field> shadowingContainer = new ShadowingContainer<>();
    fields.forEach(shadowingContainer::add);
    return shadowingContainer;
  }

  private RelationNorm createRelation(Type type, List<Column> keys, Table from, Table to) {
    TableNodeNorm fromNorm = TableNodeNorm.of(from);
    TableNodeNorm toNorm = TableNodeNorm.of(to);

    List<Expression> criteria = keys.stream()
        .map(column ->
          new ComparisonExpression(Operator.EQUAL,
              ResolvedColumn.of(fromNorm, column),
              ResolvedColumn.of(toNorm, column)))
        .collect(Collectors.toList());

    return new JoinNorm(Optional.empty(), type, fromNorm, toNorm, JoinOn.on(and(criteria)));
  }
}
