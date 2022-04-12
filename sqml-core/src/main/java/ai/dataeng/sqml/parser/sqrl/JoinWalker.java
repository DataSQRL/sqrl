package ai.dataeng.sqml.parser.sqrl;

import static ai.dataeng.sqml.parser.sqrl.analyzer.StatementAnalyzer.getCriteria;
import static ai.dataeng.sqml.util.SqrlNodeUtil.and;
import static ai.dataeng.sqml.util.SqrlNodeUtil.eq;
import static ai.dataeng.sqml.util.SqrlNodeUtil.ident;

import ai.dataeng.sqml.parser.AliasGenerator;
import ai.dataeng.sqml.parser.Column;
import ai.dataeng.sqml.parser.Field;
import ai.dataeng.sqml.parser.Relationship;
import ai.dataeng.sqml.parser.Table;
import ai.dataeng.sqml.parser.TableFactory;
import ai.dataeng.sqml.parser.sqrl.analyzer.TableBookkeeping;
import ai.dataeng.sqml.tree.AliasedRelation;
import ai.dataeng.sqml.tree.Expression;
import ai.dataeng.sqml.tree.Identifier;
import ai.dataeng.sqml.tree.Join;
import ai.dataeng.sqml.tree.Join.Type;
import ai.dataeng.sqml.tree.JoinCriteria;
import ai.dataeng.sqml.tree.JoinOn;
import ai.dataeng.sqml.tree.Query;
import ai.dataeng.sqml.tree.Relation;
import ai.dataeng.sqml.tree.TableNode;
import ai.dataeng.sqml.tree.TableSubquery;
import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.tree.name.NamePath;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.Value;

/**
 *
 */
public class JoinWalker {
  AliasGenerator gen = new AliasGenerator();
  /**
   * Walks a join path
   */
  public WalkResult walk(Name tableAlias, NamePath namePath, Optional<Relation> current,
      Map<Name, Table> joinScope) {
    Name baseTableAlias = gen.nextTableAliasName();

    Table baseTable = joinScope.get(tableAlias);
    Relation relation = current.isPresent() ? current.get() :
        new TableNode(Optional.empty(), baseTable.getId().toNamePath(), Optional.of(baseTableAlias));
    joinScope.put(baseTableAlias, baseTable);
    TableBookkeeping b = new TableBookkeeping(relation, baseTableAlias, baseTable);
    List<TableItem> tableItems = new ArrayList<>();

    for (int i = 0; i < namePath.getLength(); i++) {
      Field field = b.getCurrentTable().getField(namePath.get(i));
      if (!(field instanceof Relationship)) break;
      Relationship rel = (Relationship)field;
      Name alias = gen.nextTableAliasName();
      Relation relation1 = expandRelation(joinScope, rel, alias);
      JoinCriteria criteria = createRelCriteria(joinScope, b.getAlias(), alias, rel);
      Join join = new Join(
          Optional.empty(),
          Type.INNER,
          b.getCurrent(),
          relation1,
          Optional.of(criteria)
      );
      b = new TableBookkeeping(join, alias, rel.getToTable());
      tableItems.add(new TableItem(alias));
    }
    return new WalkResult(tableItems, b.getCurrent());
  }

  public static JoinCriteria createRelCriteria(Map<Name, Table> joinScope, Name lhs, Name rhs, Relationship rel) {
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
      Column lhsColumn = lhsTable.getEquivalent(column).orElseThrow();
      Column rhsColumn = rhsTable.getEquivalent(column).orElseThrow();
      conditions.add(eq(
          ident(lhs.toNamePath().concat(lhsColumn.getName())),
          ident(rhs.toNamePath().concat(rhsColumn.getName()))));
    }

    return new JoinOn(Optional.empty(), and(conditions));
  }

  public static JoinCriteria createTableCriteria(Map<Name, Table> joinScope, Name lhs, Name rhs) {
    //Use the lhs primary keys to join on the rhs
    Table lhsTable = joinScope.get(lhs);
    Table rhsTable = joinScope.get(rhs);
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
      TableSubquery tableSubquery = new TableSubquery(Optional.empty(), (Query)rel.getNode());
      joinScope.put(nextAlias, new TableFactory().create(tableSubquery));
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
