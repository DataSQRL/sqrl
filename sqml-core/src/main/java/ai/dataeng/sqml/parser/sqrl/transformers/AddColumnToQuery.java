package ai.dataeng.sqml.parser.sqrl.transformers;

import static ai.dataeng.sqml.util.SqrlNodeUtil.and;
import static ai.dataeng.sqml.util.SqrlNodeUtil.ident;
import static ai.dataeng.sqml.util.SqrlNodeUtil.singleColumn;
import static ai.dataeng.sqml.util.SqrlNodeUtil.toSubquery;

import ai.dataeng.sqml.parser.AliasGenerator;
import ai.dataeng.sqml.parser.Column;
import ai.dataeng.sqml.parser.Field;
import ai.dataeng.sqml.parser.Table;
import ai.dataeng.sqml.parser.sqrl.node.PrimaryKeySelectItem;
import ai.dataeng.sqml.tree.ComparisonExpression;
import ai.dataeng.sqml.tree.ComparisonExpression.Operator;
import ai.dataeng.sqml.tree.Expression;
import ai.dataeng.sqml.tree.Identifier;
import ai.dataeng.sqml.tree.Join;
import ai.dataeng.sqml.tree.Join.Type;
import ai.dataeng.sqml.tree.JoinCriteria;
import ai.dataeng.sqml.tree.JoinOn;
import ai.dataeng.sqml.tree.QuerySpecification;
import ai.dataeng.sqml.tree.Select;
import ai.dataeng.sqml.tree.SelectItem;
import ai.dataeng.sqml.tree.SingleColumn;
import ai.dataeng.sqml.tree.TableNode;
import ai.dataeng.sqml.tree.name.Name;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class AddColumnToQuery {
  AliasGenerator gen = new AliasGenerator();
  /**
   * Adds an expression and joins back to the main query, taking careful account
   * of shadowing.
   *
   * Orders.entries.discount := COALESCE(discount, 0.0);
   *
   * Expression query: SELECT _uuid, _idx1, COALESCE(discount, 0.0) AS discount$1 FROM entries;
   *
   * Transform:
   * SELECT _t1._uuid, _t1._idx1, _t1.quantity, _t1.unit_price, _t1.discount, _t2.discount AS discount$1
   * FROM entries AS _t1
   * INNER JOIN (SELECT _uuid, _idx1, COALESCE(discount, 0.0) AS discount
   *            FROM entries) AS _t2 ON _t1._uuid = _t2._uuid AND _t1._idx1 = _t2._idx1;
   */
  public QuerySpecification transform(Table table, Name expressionName, boolean isAggregating, QuerySpecification spec) {
    Name lAlias = gen.nextTableAliasName();
    TableNode tableNode = new TableNode(Optional.empty(), table.getId().toNamePath(), Optional.of(lAlias));

    Name rAlias = gen.nextTableAliasName();
    Type joinType = isAggregating? Type.LEFT : Type.INNER;
    Join join = new Join(joinType, tableNode, toSubquery(spec, rAlias), Optional.of(new JoinOn(Optional.empty(),
        toCriteria(lAlias, table.getPrimaryKeys(), rAlias, spec.getSelect()))));

    return new QuerySpecification(
        spec.getLocation(),
        toSelectList(table, expressionName, lAlias, rAlias),
        join,
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty()
    );
  }

  public static Expression toCriteria(Name lAlias, List<Column> primaryKeys, Name rAlias, Select select) {
    List<Expression> conditions = new ArrayList<>();
    for (Column column : primaryKeys) {
      PrimaryKeySelectItem rhs = select.getSelectItems().stream()
          .filter(i->i instanceof PrimaryKeySelectItem)
          .map(i->(PrimaryKeySelectItem)i)
          .filter(pk->pk.getColumn().equals(column))
          .findAny()
          .orElseThrow(()->new RuntimeException("Could not find key " + column));
      conditions.add(new ComparisonExpression(Optional.empty(), Operator.EQUAL,
          ident(lAlias.toNamePath().concat(column.getId())),
          ident(rAlias.toNamePath().concat(rhs.getAlias().get().getNamePath())))
      );
    }
    return and(conditions);
  }

  private Select toSelectList(Table table, Name expressionName, Name lAlias, Name rAlias) {
    List<SelectItem> selectItems = new ArrayList<>();
    Column c = table.fieldFactory(expressionName);
    SingleColumn column = singleColumn(rAlias.toNamePath().concat(expressionName), c.getId());
    selectItems.add(column);

    for (Field field : table.getFields().getElements()) {
      if (field instanceof Column) {
        selectItems.add(singleColumn(lAlias.toNamePath().concat(field.getName()), field.getId()));
      }
    }

    return new Select(selectItems);
  }
}
