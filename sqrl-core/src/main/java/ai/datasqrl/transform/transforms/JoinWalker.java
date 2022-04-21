package ai.datasqrl.transform.transforms;

import static ai.datasqrl.parse.util.SqrlNodeUtil.and;
import static ai.datasqrl.parse.util.SqrlNodeUtil.eq;
import static ai.datasqrl.parse.util.SqrlNodeUtil.ident;

import ai.datasqrl.parse.tree.AliasedRelation;
import ai.datasqrl.parse.tree.Expression;
import ai.datasqrl.parse.tree.Identifier;
import ai.datasqrl.parse.tree.Join;
import ai.datasqrl.parse.tree.Join.Type;
import ai.datasqrl.parse.tree.JoinCriteria;
import ai.datasqrl.parse.tree.JoinOn;
import ai.datasqrl.parse.tree.Query;
import ai.datasqrl.parse.tree.Relation;
import ai.datasqrl.parse.tree.TableNode;
import ai.datasqrl.parse.tree.TableSubquery;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.schema.Column;
import ai.datasqrl.schema.Field;
import ai.datasqrl.schema.Relationship;
import ai.datasqrl.schema.Table;
import ai.datasqrl.schema.factory.SubqueryTableFactory;
import ai.datasqrl.util.AliasGenerator;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.Value;

public class JoinWalker {

  AliasGenerator gen = new AliasGenerator();

  /**
   * Walks a join path
   */
  public WalkResult walk(Name baseTableAlias, Optional<Name> lastAlias, NamePath namePath,
      Optional<Relation> current,
      Optional<JoinCriteria> lastCriteria,
      //Criteria is lazy because the join aliases don't exist yet to be evaluated
      Map<Name, Table> joinScope) {

    Table baseTable = joinScope.get(baseTableAlias);
    Relation relation = current.isPresent()
        ? current.get() :
        new TableNode(Optional.empty(), baseTable.getId().toNamePath(),
            Optional.of(baseTableAlias));

    TableBookkeeping b = new TableBookkeeping(relation, baseTableAlias, baseTable);
    List<TableItem> tableItems = new ArrayList<>();

    for (int i = 0; i < namePath.getLength(); i++) {
      Field field = b.getCurrentTable().getField(namePath.get(i));
      Preconditions.checkNotNull(field);
      if (!(field instanceof Relationship)) {
        break;
      }
      Relationship rel = (Relationship) field;

      Name alias = i == namePath.getLength() - 1
          ? lastAlias.orElseGet(() -> gen.nextTableAliasName())
          : gen.nextTableAliasName();

      Relation relation1 = expandRelation(joinScope, rel, alias);
      JoinOn criteria = createRelCriteria(joinScope, b.getAlias(), alias, rel);

      Optional<JoinCriteria> additionalCriteria = i == namePath.getLength() - 1
          ? lastCriteria
          : Optional.empty();

      Join join = new Join(
          Optional.empty(),
          Type.INNER,
          b.getCurrent(),
          relation1,
          Optional.of(merge(criteria, additionalCriteria))
      );
      b = new TableBookkeeping(join, alias, rel.getToTable());
      tableItems.add(new TableItem(alias));
    }
    return new WalkResult(tableItems, b.getCurrent());
  }

  private JoinCriteria merge(JoinOn criteria, Optional<JoinCriteria> additionalCriteria) {
    if (additionalCriteria.isEmpty()) {
      return criteria;
    }

    return new JoinOn(criteria.getLocation(), and(criteria.getExpression(),
        additionalCriteria.map(e -> ((JoinOn) e).getExpression())));
  }

  public static JoinOn createRelCriteria(Map<Name, Table> joinScope, Name lhs, Name rhs,
      Relationship rel) {
    //Use the lhs primary keys to join on the rhs
    Table lhsTable = joinScope.get(lhs);
    Table rhsTable = joinScope.get(rhs);

    List<Column> joinColumns;
    if (rel.type == Relationship.Type.CHILD) {
      joinColumns = rel.getTable().getPrimaryKeys();
    } else if (rel.type == Relationship.Type.PARENT) {
      joinColumns = rel.getToTable().getPrimaryKeys();
    } else if (rel.type == Relationship.Type.JOIN) {
      joinColumns = rel.getTable().getPrimaryKeys(); //converted to table in prior step
    } else {
      throw new RuntimeException("Unknown join type");
    }

    List<Expression> conditions = new ArrayList<>();
    for (Column column : joinColumns) {
      Column lhsColumn = lhsTable.getEquivalent(column).orElseThrow(() ->
          new RuntimeException("Could not find column: " + column + " in " + lhsTable));
      Column rhsColumn = rhsTable.getEquivalent(column)
          .orElseThrow(() ->
              new RuntimeException("Could not find column: " + column + " in " + rhsTable));
      conditions.add(eq(
          ident(lhs.toNamePath().concat(lhsColumn.getName())),
          ident(rhs.toNamePath().concat(rhsColumn.getName()))));
    }

    return new JoinOn(Optional.empty(), and(conditions));
  }

  public static JoinCriteria createTableCriteria(Map<Name, Table> joinScope, Name lhs, Name rhs) {
    //Use the lhs primary keys to join on the rhs
    Table lhsTable = joinScope.get(lhs);
    Preconditions.checkNotNull(lhsTable, "Could not find join scope table %s", lhs);
    Table rhsTable = joinScope.get(rhs);
    Preconditions.checkNotNull(lhsTable, "Could not find join scope table %s", rhs);
    List<Expression> conditions = new ArrayList<>();

    for (Column lhsColumn : lhsTable.getPrimaryKeys()) {
      Column rhsColumn = rhsTable.getEquivalent(lhsColumn).orElseThrow();
      conditions.add(eq(
          ident(lhs.toNamePath().concat(lhsColumn.getName())),
          ident(rhs.toNamePath().concat(rhsColumn.getName()))));
    }

    return new JoinOn(Optional.empty(), and(conditions));
  }

  public static Relation expandRelation(
      Map<Name, Table> joinScope,
      Relationship rel, Name nextAlias) {
    if (rel.getType() == Relationship.Type.JOIN) {
      joinScope.put(nextAlias, new SubqueryTableFactory().create((Query) rel.getNode()));
      TableSubquery tableSubquery = new TableSubquery(Optional.empty(), (Query) rel.getNode());
      return new AliasedRelation(
          Optional.empty(),
          tableSubquery,
          new Identifier(Optional.empty(), nextAlias.toNamePath())
      );
    } else {
      joinScope.put(nextAlias, rel.getToTable());
      return new TableNode(
          Optional.empty(),
          rel.getToTable().getId().toNamePath(),
          Optional.of(nextAlias)
      );
    }
  }

  @Value
  public class WalkResult {

    List<TableItem> tableStack;
    Relation relation;
  }

  @Value
  public class TableItem {

    Name alias;
  }
}
