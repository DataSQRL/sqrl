package ai.datasqrl.plan.local.transpiler.nodes.relation;

import ai.datasqrl.parse.tree.AstVisitor;
import ai.datasqrl.parse.tree.Expression;
import ai.datasqrl.parse.tree.GroupBy;
import ai.datasqrl.parse.tree.Limit;
import ai.datasqrl.parse.tree.Node;
import ai.datasqrl.parse.tree.NodeLocation;
import ai.datasqrl.parse.tree.OrderBy;
import ai.datasqrl.parse.tree.SingleColumn;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.plan.local.transpiler.nodes.expression.ReferenceExpression;
import ai.datasqrl.plan.local.transpiler.nodes.expression.ResolvedColumn;
import ai.datasqrl.plan.local.transpiler.nodes.expression.ResolvedFunctionCall;
import ai.datasqrl.plan.local.transpiler.nodes.node.SelectNorm;
import java.util.List;
import java.util.Map;
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

  private List<Expression> primaryKeys;

  public QuerySpecNorm(Optional<NodeLocation> location, List<ResolvedColumn> parentPrimaryKeys,
      List<Expression> addedPrimaryKeys, SelectNorm select, RelationNorm from,
      Optional<Expression> where,
      Optional<GroupBy> groupBy, Optional<Expression> having, Optional<OrderBy> orderBy,
      Optional<Limit> limit, List<Expression> primaryKeys) {
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
    this.primaryKeys = primaryKeys;
  }

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

  //todo: cleanup
  @Override
  public Name getFieldName(Expression references) {
    for (SingleColumn col : this.getSelect().getSelectItems()) {
      if (col.getExpression().equals(references)) {
        if (col.getAlias().isEmpty()) {
          if (references instanceof ReferenceExpression) {
            return getFieldName(((ReferenceExpression)references).getReferences());
          } else if (references instanceof ResolvedColumn) {
            return ((ResolvedColumn) references).getColumn().getId();
          }
        }
        return col.getAlias().get().getNamePath().getLast();
      }
    }

    for (ResolvedColumn column : this.parentPrimaryKeys) {
      if (references == column) {
        return column.getColumn().getId();
      }
    }

    if (references instanceof ReferenceExpression) {
      return getFieldName(((ReferenceExpression)references).getReferences());
    } else if (references instanceof ResolvedColumn) {
//      return ((ResolvedColumn) references).getColumn().getId();
    }

    log.warn("Name should be aliased for {}", references);
    return Name.system("<??>");
  }

  @Override
  public List<Expression> getFields() {
    return this.select.getSelectItems().stream()
        .map(ex-> new ReferenceExpression(this, ex.getExpression()))
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
    for (SingleColumn col : this.getSelect().getSelectItems()) {
      if (col.getAlias().get().getNamePath().equals(namePath)) {
        return Optional.of(col.getExpression());
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