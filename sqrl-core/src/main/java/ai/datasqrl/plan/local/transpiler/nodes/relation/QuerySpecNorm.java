package ai.datasqrl.plan.local.transpiler.nodes.relation;

import ai.datasqrl.parse.tree.AstVisitor;
import ai.datasqrl.parse.tree.Expression;
import ai.datasqrl.parse.tree.GroupBy;
import ai.datasqrl.parse.tree.Limit;
import ai.datasqrl.parse.tree.Node;
import ai.datasqrl.parse.tree.NodeLocation;
import ai.datasqrl.parse.tree.OrderBy;
import ai.datasqrl.parse.tree.SelectItem;
import ai.datasqrl.parse.tree.SingleColumn;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.plan.local.transpiler.nodes.expression.ReferenceExpression;
import ai.datasqrl.plan.local.transpiler.nodes.expression.ResolvedColumn;
import ai.datasqrl.plan.local.transpiler.nodes.expression.ResolvedFunctionCall;
import ai.datasqrl.plan.local.transpiler.nodes.node.SelectNorm;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

/**
 * An normalized version of {@link ai.datasqrl.parse.tree.QuerySpecification}
 */
@Getter
@ToString
@Slf4j
@EqualsAndHashCode(callSuper = false)
public class QuerySpecNorm extends RelationNorm {
  private List<ResolvedColumn> parentPrimaryKeys;
  private List<Expression> addedPrimaryKeys;

  private SelectNorm select;
  private RelationNorm from;
  private Optional<Expression> where;
  private Optional<GroupBy> groupBy;
  private Optional<Expression> having;
  private Optional<OrderBy> orderBy;
  private Optional<Limit> limit;

  private Map<Expression, Name> nameMap;
  private List<Expression> primaryKeys;

  public QuerySpecNorm(Optional<NodeLocation> location, List<ResolvedColumn> parentPrimaryKeys,
      List<Expression> addedPrimaryKeys, SelectNorm select, RelationNorm from,
      Optional<Expression> where,
      Optional<GroupBy> groupBy, Optional<Expression> having, Optional<OrderBy> orderBy,
      Optional<Limit> limit, Map<Expression, Name> nameMap, List<Expression> primaryKeys) {
    super(location);
    this.parentPrimaryKeys = parentPrimaryKeys;
    this.addedPrimaryKeys = addedPrimaryKeys;
    this.select = select;
    this.from = from;
    this.where = where;
    this.groupBy = groupBy;
    this.having = having;
    this.orderBy = orderBy;
    this.limit = limit;
    this.nameMap = nameMap;
    this.primaryKeys = primaryKeys;
  }

  //  private final boolean isAggregating;

//  private final boolean isDistinct;
//  List<Expression> select;
//  RelationNorm from;
//  private final List<JoinNorm> addlJoins; //join before passing here...
//  Optional<Expression> where;
//  Set<ReferenceExpression> groupBy;
//  Optional<Expression> having; //replace repeated expressions with ReferenceOrdinal
//  List<SortItem> orders; //replace repeated expressions with ReferenceOrdinal
//  Optional<Integer> limit;
//
//  public QuerySpecNorm(boolean isAggregating, List<ResolvedColumn> parentPrimaryKeys,
//      List<Expression> addedPrimaryKeys, boolean isDistinct,
//      List<Expression> select, RelationNorm from, List<JoinNorm> addlJoins, Optional<Expression> where,
//      Set<ReferenceExpression> groupBy, Optional<Expression> having, List<SortItem> orders,
//      Optional<Integer> limit, Map<Expression, Name> nameMap, List<Expression> primaryKeys) {
//    super();
//    this.isAggregating = isAggregating;
//    this.parentPrimaryKeys = parentPrimaryKeys;
//    this.addedPrimaryKeys = addedPrimaryKeys;
//    this.isDistinct = isDistinct;
//    this.select = select;
//    this.from = from;
//    this.addlJoins = addlJoins;
//    this.where = where;
//    this.groupBy = groupBy;
//    this.having = having;
//    this.orders = orders;
//    this.limit = limit;
//    this.nameMap = nameMap;
//    this.primaryKeys = primaryKeys;
//  }

  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitQuerySpecNorm(this, context);
  }

  @Override
  public List<? extends Node> getChildren() {
    return List.of();
  }

  @Override
  public RelationNorm getLeftmost() {
    return this;
  }

  @Override
  public RelationNorm getRightmost() {
    return this;
  }

  @Override
  public Name getFieldName(Expression references) {
    Name name = this.nameMap.get(references);
    if (name == null) {
      if (references instanceof ReferenceExpression) {
        return getFieldName(((ReferenceExpression)references).getReferences());
      } else if (references instanceof ResolvedColumn) {
        return ((ResolvedColumn) references).getColumn().getId();
      }

      log.warn("Name should be aliased for {}", references);
      return Name.system("<??>");
    }
    return name;
  }

  @Override
  public List<Expression> getFields() {
    return this.select.getSelectItems().stream()
        .map(ex-> new ReferenceExpression(this, ((SingleColumn)ex).getExpression()))
        .collect(Collectors.toList());
  }

  @Override
  public List<Expression> getPrimaryKeys() {
    return this.primaryKeys;
  }

  @Override
  public Optional<Expression> walk(NamePath namePath) {
    if (namePath.getLength() > 1) {
      return Optional.empty();
    }
    for (Entry<Expression, Name> name : this.nameMap.entrySet()) {
      if (name.getValue().equals(namePath.getLast())) {
        return Optional.of(name.getKey());
      }
    }

    return Optional.empty();
  }

  public Expression getField(NamePath namePath) {
    return walk(namePath).get();
  }


  public boolean isAggregating() {
    for (SingleColumn column : select.getSelectItems()) {
      if (column.getExpression() instanceof ResolvedFunctionCall &&
          ((ResolvedFunctionCall) column.getExpression()).getFunction().isAggregate()) {
        return true;
      }
    }
    return false;
  }
}