package org.apache.calcite.rel.rel2sql;

import com.datasqrl.engine.stream.flink.sql.calcite.FlinkDialect;
import com.datasqrl.engine.stream.flink.sql.model.QueryPipelineItem;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import lombok.Getter;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Snapshot;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.core.Uncollect;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.sql.JoinConditionType;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlSnapshot;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.flink.calcite.shaded.com.google.common.collect.ImmutableList;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;
import org.apache.flink.table.planner.plan.nodes.calcite.LogicalWatermarkAssigner;

/**
 * The original RelToSqlConverter
 * DO NOT REMOVE
 */
@Getter
public class FlinkRelToSqlConverter extends RelToSqlConverter {

  private final AtomicInteger uniqueTableId;
  List<QueryPipelineItem> queries = new ArrayList<>();

  public FlinkRelToSqlConverter(AtomicInteger uniqueTableId) {
    super(FlinkDialect.DEFAULT);
    this.uniqueTableId = uniqueTableId;
  }

  public QueryPipelineItem create(SqlNode node) {
    QueryPipelineItem q = new QueryPipelineItem(node, "table$"+ (uniqueTableId.incrementAndGet()));
    queries.add(q);
    return q;
  }

  public Result visit(LogicalFilter e) {
    return super.visit((Filter) e);
  }

  //Snapshot field name is a field from the lhs
  // like a lateral join

  public Result visit(LogicalCorrelate e) {
    //special case: check for unnest and return a cross join
    if (e.getRight() instanceof Uncollect) {
      return rewriteUncollect(e, (Uncollect) e.getRight());
    } else if (e.getRight() instanceof Filter &&
        ((LogicalFilter) e.getRight()).getInput() instanceof Snapshot) {
      return rewriteSnapshot(e, (LogicalFilter) e.getRight());
    } else {
      return super.visit(e);
    }
  }

  /**
   *
   * Event Time Temporal Join.
   *
   * SELECT [column_list]
   * FROM table1 [AS <alias1>]
   * [LEFT] JOIN table2 FOR SYSTEM_TIME AS OF table1.{ proctime | rowtime } [AS <alias2>]
   * ON table1.column-name1 = table2.column-name1
   *
   * The input is a correlated join (because it needs to be) but we want it as a normal join.
   * We will push the filter into the snapshot as a join condition.
   *
   * [correlate]
   *   [relnode]
   *   [filter]
   *     [snapshot]
   *       [rhs]
   *
   * becomes sql like:
   *
   * [join ON filter]
   *   [relnode]
   *   [snapshot]
   *     [tableRef]
   * +
   * tableref <- [rhs]
   *
   *
   */
  private Result rewriteSnapshot(LogicalCorrelate coor, LogicalFilter filter) {
    Snapshot snapshot = (Snapshot) filter.getInput();

    //1 visit snapshot input and put in subquery
    Result snapshotInput = visitInput(snapshot, 0);

    SqlNode stmt = snapshotInput.asStatement();

    QueryPipelineItem q = create(stmt);

    SqlNode tableRef = new SqlIdentifier(q.getTableName(), SqlParserPos.ZERO);

    //2 create snapshot node, walk left first
    final Result leftResult =
        visitInput(coor, 0)
            .resetAlias(coor.getCorrelVariable(), coor.getRowType());
    for (CorrelationId id : coor.getVariablesSet()) {
      correlTableMap.put(id, leftResult.qualifiedContext());
    }

    SqlNode period = new SqlIdentifier(List.of(leftResult.neededAlias,
        ((RexFieldAccess) snapshot.getPeriod()).getField().getName()), SqlParserPos.ZERO);
    SqlSnapshot snapshotNode = new SqlSnapshot(SqlParserPos.ZERO,
        tableRef, period);
    final SqlNode rightAs =
        SqlStdOperatorTable.AS.createCall(POS, snapshotNode,
            new SqlIdentifier(snapshotInput.neededAlias, POS));

    //last create join

    JoinType joinType = joinType(coor.getJoinType());

    for (CorrelationId id : filter.getVariablesSet()) {
      correlTableMap.put(id, snapshotInput.qualifiedContext());
    }

    final SqlNode join =
        new SqlJoin(POS,
            leftResult.asFrom(),
            SqlLiteral.createBoolean(false, POS),
            joinType.symbol(POS),
            rightAs,
            JoinConditionType.ON.symbol(POS),
            snapshotInput.qualifiedContext().toSql(null, filter.getCondition()));

    leftResult.resetAlias();

    return result(join, leftResult.resetAlias(), snapshotInput);
  }

  private Result rewriteUncollect(LogicalCorrelate e, Uncollect uncollect) {
    final Result leftResult =
        visitInput(e, 0)
            .resetAlias(e.getCorrelVariable(), e.getRowType());
    for (CorrelationId id : e.getVariablesSet()) {
      correlTableMap.put(id, leftResult.qualifiedContext());
    }
    final Result rightResult = visitInput(e, 1);

    final Result leftResult2 = leftResult.resetAlias();

    RexFieldAccess fieldAccess = (RexFieldAccess) ((LogicalProject) uncollect.getInput()).getProjects()
        .get(0);
    SqlCall call = SqlStdOperatorTable.UNNEST.createCall(SqlParserPos.ZERO,
        new SqlIdentifier(List.of(leftResult2.neededAlias, fieldAccess.getField().getName()),
            SqlParserPos.ZERO));
    List<SqlNode> ops = new ArrayList<>();
    ops.add(call);
    ops.add(new SqlIdentifier(rightResult.neededAlias, POS));
    ops.addAll(uncollect.getRowType().getFieldNames().stream()
        .map(r->new SqlIdentifier(r, SqlParserPos.ZERO))
        .collect(Collectors.toList()));

    final SqlNode rightLateralAs =
        SqlStdOperatorTable.AS.createCall(POS, ops.toArray(SqlNode[]::new));

    final SqlNode join =
        new SqlJoin(POS,
            leftResult2.asFrom(),
            SqlLiteral.createBoolean(false, POS),
            JoinType.COMMA.symbol(POS),
            rightLateralAs,
            JoinConditionType.NONE.symbol(POS),
            null);
    return result(join, leftResult2, rightResult);
  }

  public Result visit(LogicalWatermarkAssigner e) {
    return dispatch(e.getInput());
  }

  //Accessed via reflection
//  public Result visit(LogicalStream stream) {
//    Result x = super.visitInput(stream, 0)
//        .resetAlias();
//
//    SqlSelect select = x.asSelect();
//    QueryPipelineItem queries1 = getOrCreate(QueryType.STREAM, select, stream.getInput(0), stream);
//
//    SqlIdentifier table = new SqlIdentifier(queries1.getTableName(), SqlParserPos.ZERO);
//    SqlIdentifier identifier = table;
//    return this.result(identifier, ImmutableList.of(Clause.FROM), stream, (Map)null);
//  }

  @Override
  public Result visit(Project project) {
    Result result = super.visit(project);

    //Don't remove the project as it may have other expressions
    return result;
  }

  public Result visit(Snapshot e) {
    LogicalProject tbl = (LogicalProject) e.getInput();
    Result x = dispatch(tbl);

    SqlNode period = new SqlIdentifier(List.of(x.neededAlias,
        ((RexFieldAccess) e.getPeriod()).getField().getName()), SqlParserPos.ZERO);

    SqlNode stmt = x.asStatement();
    QueryPipelineItem q = create(stmt);
    SqlNode tableRef = new SqlIdentifier(q.getTableName(), SqlParserPos.ZERO);
    SqlSnapshot snapshot = new SqlSnapshot(SqlParserPos.ZERO,
        tableRef, period);

    final SqlNode rightLateralAs =
        SqlStdOperatorTable.AS.createCall(POS, snapshot,
            new SqlIdentifier(x.neededAlias, POS));

    return result(rightLateralAs, List.of(Clause.FROM), null, null, Map.of());
  }

  @Override
  public Result visit(TableFunctionScan e) {
    if (e.getInputs().isEmpty()) {
      return super.visit(e).resetAlias();
    }

    Result x = super.visitInput(e, 0)
        .resetAlias();

    SqlSelect select = x.asSelect();
    QueryPipelineItem queries1 = create(select);

    RexBuilder rex = new RexBuilder(new FlinkTypeFactory(this.getClass().getClassLoader(),
        FlinkTypeSystem.INSTANCE));
    //The first flink operand is referencing something else
    // replace it with a placeholder then replace it with the sql node
    RexCall call = (RexCall) e.getCall();
    List<RexNode> rexOperands = new ArrayList<>(call.getOperands());
    RexNode placeholder = rex.makeLiteral(true);
    rexOperands.set(0, placeholder);

    RexCall cloned = call.clone(call.getType(), rexOperands);

    SqlBasicCall tableCall = (SqlBasicCall) x.qualifiedContext().toSql((RexProgram) null, cloned);
    rewriteDescriptor(tableCall);

    SqlNode tableRef = SqlStdOperatorTable.EXPLICIT_TABLE.createCall(SqlParserPos.ZERO,
        new SqlIdentifier(queries1.getTableName(), SqlParserPos.ZERO));
    tableCall.setOperand(0, tableRef);

    SqlNode collect = SqlStdOperatorTable.COLLECTION_TABLE.createCall(SqlParserPos.ZERO, tableCall);

    return this.result(collect, ImmutableList.of(Clause.SELECT), e, (Map) null);
  }

  private void rewriteDescriptor(SqlBasicCall call) {
    if (
        call.getOperator().equals(FlinkSqlOperatorTable.TUMBLE) ||
            call.getOperator().equals(FlinkSqlOperatorTable.HOP) ||
            call.getOperator().equals(FlinkSqlOperatorTable.CUMULATE)
    ) {
      SqlBasicCall descriptorCall = (SqlBasicCall) call.getOperandList().get(1);
      descriptorCall.setOperand(0,
          simplifyName((SqlIdentifier) descriptorCall.getOperandList().get(0)));
    }
  }

  private SqlNode simplifyName(SqlIdentifier identifier) {
    return new SqlIdentifier(identifier.names.get(identifier.names.size() - 1), SqlParserPos.ZERO);
  }
}
