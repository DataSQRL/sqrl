package ai.datasqrl.plan.local.transpiler.nodes.relation;

import ai.datasqrl.parse.tree.AstVisitor;
import ai.datasqrl.parse.tree.Expression;
import ai.datasqrl.parse.tree.GroupBy;
import ai.datasqrl.parse.tree.Limit;
import ai.datasqrl.parse.tree.Node;
import ai.datasqrl.parse.tree.NodeLocation;
import ai.datasqrl.parse.tree.OrderBy;
import ai.datasqrl.parse.tree.SingleColumn;
import ai.datasqrl.parse.tree.Window;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.plan.calcite.SqrlOperatorTable;
import ai.datasqrl.plan.local.transpiler.nodes.expression.ReferenceExpression;
import ai.datasqrl.plan.local.transpiler.nodes.expression.ReferenceOrdinal;
import ai.datasqrl.plan.local.transpiler.nodes.expression.ResolvedColumn;
import ai.datasqrl.plan.local.transpiler.nodes.expression.ResolvedFunctionCall;
import ai.datasqrl.plan.local.transpiler.nodes.node.SelectNorm;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Streams;
import java.util.List;
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

  public QuerySpecNorm(Optional<NodeLocation> location, List<ResolvedColumn> parentPrimaryKeys,
      List<Expression> addedPrimaryKeys, SelectNorm select, RelationNorm from,
      Optional<Expression> where,
      Optional<GroupBy> groupBy, Optional<Expression> having, Optional<OrderBy> orderBy,
      Optional<Limit> limit) {
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
    // Check group by
    if (isAggregating()) {
      Iterable<Expression> expressions = Iterables.concat(this.getParentPrimaryKeys(),
          this.getAddedPrimaryKeys(),
          this.groupBy.map(g->unpackOrdinals(g.getGroupingElement().getExpressions())).orElse(List.of()));
      return Lists.newArrayList(expressions);
    }
    // Check distinct
    if (this.select.isDistinct()) {
      Iterable<Expression> expressions = Iterables.concat(this.getParentPrimaryKeys(),
          this.getAddedPrimaryKeys(), this.getSelect().getAsExpressions());
      return Lists.newArrayList(expressions);
    }
    // Check rownum
    SingleColumn col;
    if (this.from instanceof QuerySpecNorm && (col = ((QuerySpecNorm) from).getRowNumField()) != null &&
      checkRowNumFilter(col)) {

      ResolvedFunctionCall functionCall = (ResolvedFunctionCall)col.getExpression();
      Window window = functionCall.getOver().get();
      return window.getPartitionBy();
    }

    // else derive new PK
    return from.getPrimaryKeys();
  }

  //TODO: Fix me. Assume true for now because user's can't specify this in the script.
  private boolean checkRowNumFilter(SingleColumn col) {
    return true;
  }

  private List<Expression> unpackOrdinals(List<Expression> expressions) {
    return expressions.stream()
        .map(ex -> (ex instanceof ReferenceOrdinal)
            ? this.select.getSelectItems().get(((ReferenceOrdinal)ex).getOrdinal()).getExpression()
            : ex)
        .collect(Collectors.toList());
  }

  protected SingleColumn getRowNumField() {
    for (SingleColumn col : getSelect().getSelectItems()) {
      if (col.getExpression() instanceof ResolvedFunctionCall &&
          ((ResolvedFunctionCall)col.getExpression()).getFunction().getSqrlName()
              .equals(Name.system(SqrlOperatorTable.ROW_NUMBER.getName()))) {
        return col;
      }
    }
    return null;
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
          ((ResolvedFunctionCall) column.getExpression()).getFunction().isAggregate() &&
          !((ResolvedFunctionCall) column.getExpression()).getFunction().requiresOver()
      ) {

        return true;
      }
    }
    return false;
  }

  public boolean isPrimaryKey(Expression expr) {
    return getPrimaryKeys().contains(expr);
  }

  public boolean isParentPrimaryKey(Expression expr) {
    return getParentPrimaryKeys().contains(expr);
  }

  public boolean isVisible(Expression expr) {
    return !isParentPrimaryKey(expr) && !getAddedPrimaryKeys().contains(expr);
  }

  public List<Expression> getAllColumns() {
    return Streams.concat(parentPrimaryKeys.stream(),
        addedPrimaryKeys.stream(),
        select.getSelectItems().stream().map(e->e.getExpression()))
        .collect(Collectors.toList());
  }
}