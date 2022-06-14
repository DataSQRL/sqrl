package ai.datasqrl.schema;

import ai.datasqrl.function.SqlNativeFunction;
import ai.datasqrl.parse.tree.ComparisonExpression;
import ai.datasqrl.parse.tree.ComparisonExpression.Operator;
import ai.datasqrl.parse.tree.Expression;
import ai.datasqrl.parse.tree.Identifier;
import ai.datasqrl.parse.tree.Join.Type;
import ai.datasqrl.parse.tree.JoinOn;
import ai.datasqrl.parse.tree.Limit;
import ai.datasqrl.parse.tree.LongLiteral;
import ai.datasqrl.parse.tree.OrderBy;
import ai.datasqrl.parse.tree.Relation;
import ai.datasqrl.parse.tree.SingleColumn;
import ai.datasqrl.parse.tree.SortItem;
import ai.datasqrl.parse.tree.SortItem.Ordering;
import ai.datasqrl.parse.tree.Window;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.plan.local.transpiler.nodes.expression.ReferenceExpression;
import ai.datasqrl.plan.local.transpiler.nodes.expression.ResolvedFunctionCall;
import ai.datasqrl.plan.local.transpiler.nodes.node.SelectNorm;
import ai.datasqrl.plan.local.transpiler.nodes.relation.JoinNorm;
import ai.datasqrl.plan.local.transpiler.nodes.relation.QuerySpecNorm;
import ai.datasqrl.plan.local.transpiler.nodes.relation.RelationNorm;
import ai.datasqrl.plan.local.transpiler.nodes.relation.TableNodeNorm;
import ai.datasqrl.plan.local.transpiler.nodes.schemaRef.TableOrRelationship;
import ai.datasqrl.plan.local.transpiler.util.CriteriaUtil;
import ai.datasqrl.plan.local.transpiler.util.RelationNormRewriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import lombok.Getter;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

@Getter
public class Relationship extends Field {

  private final Table table;
  private final Table toTable;
  private final JoinType joinType;
  private final Multiplicity multiplicity;

  private final Relation relation;
  private final Optional<OrderBy> orders;
  //Requires a transformation to limit cardinality to one
  private final Optional<Limit> limit;

  public Relationship(Name name, Table table, Table toTable, JoinType joinType,
      Multiplicity multiplicity, Relation relation, Optional<OrderBy> orders,
      Optional<Limit> limit) {
    super(name);
    this.table = table;
    this.toTable = toTable;
    this.joinType = joinType;
    this.multiplicity = multiplicity;
    this.relation = relation;
    this.orders = orders;
    this.limit = limit;
  }

  public Relationship(Name name, Table table, Table toTable, JoinType joinType,
                      Multiplicity multiplicity, Relation relation) {
    this(name,table,toTable,joinType,multiplicity,relation, Optional.empty(), Optional.empty());
  }

  @Override
  public Name getId() {
    return name;
  }

  @Override
  public int getVersion() {
    return 0;
  }

  public RelationNorm getRelation() {
    JoinNorm joinNorm = (JoinNorm) relation.accept(new RelationNormRewriter(), null);

    if (limitTransformationRequired()) {
      return transformLimit(joinNorm);
    }

    return joinNorm;
  }

  /**
   *  joinNorm.leftmost INNER JOIN (
   *   SELECT f1, f2, f3, ...
   *   FROM (
   *     SELECT f1, f2, f3,
   *            ROW_NUMBER() OVER (PARTITION BY leftMost.pk1, leftMost.pk2, ...
   *                               ORDER BY sortItem1 dir1, sortItem2 dir2, ...) _row_num
   *     FROM joinNorm
   *   )
   *   WHERE _row_num = 1
   *  ) ON leftMost.pk1, leftMost.pk2, ...
   */
  private RelationNorm transformLimit(JoinNorm joinNorm) {
    TableOrRelationship tableRef = ((TableNodeNorm)joinNorm.getLeftmost()).getRef();
    TableNodeNorm base = new TableNodeNorm(Optional.empty(), tableRef.getTable().getPath(),
        Optional.empty(), tableRef, false, List.of());

    /**
     *     SELECT rightmost.f1, rightmost.f2, ...
     *            ROW_NUMBER() OVER (PARTITION BY leftMost.pk1, leftMost.pk2, ...
     *                               ORDER BY sortItem1 dir1, sortItem2 dir2, ...) _row_num
     *     FROM joinNorm
     */
    Expression row_num = rowNumFnc(joinNorm.getLeftmost());
    Map<Expression, Name> alias = new HashMap<>(Map.of(row_num, Name.system("__ROW_NUM")));
    List<Expression> primaryKeys = joinNorm.getLeftmost().getPrimaryKeys();

    QuerySpecNorm inner = new QuerySpecNorm(
        Optional.empty(),
        List.of(),
        List.of(),
        toSelectList(primaryKeys, joinNorm.getRightmost().getFields(), Optional.of(row_num)),
        joinNorm,
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty()
    );

    ReferenceExpression ref = new ReferenceExpression(inner, row_num);
    /**
     SELECT f1, f2, f3, ...
     *   FROM (
     *     inner
     *   )
     *   WHERE _row_num = 1
     */
    List<Expression> fields = inner.getFields();
    int pkSize = primaryKeys.size();
    List<Expression> pks = fields.subList(0, pkSize);
    List<Expression> refPks = pks.stream().map(pk->new ReferenceExpression(inner, pk)).collect(
        Collectors.toList());
    List<Expression> selectList = fields.subList(pkSize, fields.size() - 1);
    QuerySpecNorm outer = new QuerySpecNorm(Optional.empty(),
        List.of(),
        refPks,
        toSelectList(List.of(), selectList, Optional.empty()),
        inner,
        Optional.of(filterRowNum(ref)),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty()
    );

    return new JoinNorm(Optional.empty(),
        Type.INNER,
        base,
        outer,
        JoinOn.on(CriteriaUtil.sameTableEq(base, outer))
    );
  }

  private Expression filterRowNum(Expression ref) {

    return new ComparisonExpression(Operator.LESS_THAN_OR_EQUAL, ref,
        new LongLiteral(this.limit.get().getValue()));
  }

  private Expression rowNumFnc(RelationNorm table) {
    ResolvedFunctionCall resolvedFunctionCall = new ResolvedFunctionCall(Optional.empty(),
        Name.system(SqlStdOperatorTable.ROW_NUMBER.getName()).toNamePath(),
        List.of(),
        false,
        Optional.of(createWindow(table)),
      null,
        new SqlNativeFunction(SqlStdOperatorTable.ROW_NUMBER)
    );
    return resolvedFunctionCall;
  }

  private Window createWindow(RelationNorm table) {
    Window window = new Window(toPartition(table),
        Optional.of(new OrderBy(List.of(
            new SortItem(
                new Identifier(
                    Optional.empty(),
                    NamePath.parse("_ingest_time")),
                Ordering.DESCENDING)))));
    return window;
  }

  private List<Expression> toPartition(RelationNorm table) {
    return table.getPrimaryKeys();
  }

  private SelectNorm toSelectList(List<Expression> primaryKeys, List<Expression> in, Optional<Expression> rowNumFnc) {
    Set<Expression> fields = new HashSet<>(primaryKeys);
    fields.addAll(in);
    List<SingleColumn> columns = fields.stream()
        .map(f->new SingleColumn(f))
        .collect(Collectors.toList());

    rowNumFnc.ifPresent(f->columns.add(new SingleColumn(f, SingleColumn.alias("__ROW_NUM"))));

    return new SelectNorm(Optional.empty(), false, columns);
  }

  private boolean limitTransformationRequired() {
    return limit.isPresent();
  }

  public enum JoinType {
    PARENT, CHILD, JOIN
  }

  public enum Multiplicity {
    ZERO_ONE, ONE, MANY
  }
}