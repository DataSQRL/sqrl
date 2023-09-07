package com.datasqrl.calcite.schema;

import com.datasqrl.calcite.QueryPlanner;
import com.datasqrl.calcite.SqrlPreparingTable;
import com.datasqrl.calcite.SqrlRelBuilder;
import com.datasqrl.util.CalciteUtil.RelDataTypeFieldBuilder;
import java.util.ArrayList;
import java.util.List;
import java.util.Stack;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.AllArgsConstructor;
import lombok.Value;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory.FieldInfoBuilder;
import org.apache.calcite.sql.JoinConditionType;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlUserDefinedTableFunction;
import org.apache.commons.collections.ListUtils;

@AllArgsConstructor
public class SqlJoinPathBuilder {
  QueryPlanner planner;
  final List<Frame> tableHistory = new ArrayList<>();
  final Stack<Frame> stack = new Stack<>();

  final AtomicInteger aliasInt = new AtomicInteger(0);

  /**
   *
   */
  public SqlJoinPathBuilder scanFunction(List<String> path, List<SqlNode> args) {
    SqlUserDefinedTableFunction op = SqrlRelBuilder.getSqrlTableFunction(planner, path);
    if (op == null && args.isEmpty()) {
      scanNestedTable(path);
      return this;
    }

    scanFunction(op, args);
    return this;
  }

  public SqlJoinPathBuilder scanFunction(SqlUserDefinedTableFunction op,
      List<SqlNode> args) {
    RelDataType type = op.getFunction().getRowType(planner.getTypeFactory(), List.of());
    SqlCall call = op.createCall(SqlParserPos.ZERO, args);

    String alias = "_t"+aliasInt.incrementAndGet();
    call = SqlStdOperatorTable.COLLECTION_TABLE.createCall(SqlParserPos.ZERO, call);

    SqlCall aliasedCall = SqlStdOperatorTable.AS.createCall(SqlParserPos.ZERO, call, new SqlIdentifier(alias, SqlParserPos.ZERO));
    Frame frame = new Frame(type, aliasedCall, alias);
    stack.push(frame);
    tableHistory.add(frame);

    return this;
  }


  public SqlJoinPathBuilder joinLateral() {
    Frame right = stack.pop();
    Frame left = stack.pop();

    SqlJoin join = new SqlJoin(SqlParserPos.ZERO,
        left.getNode(),
        SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
        JoinType.DEFAULT.symbol(SqlParserPos.ZERO),
        SqlStdOperatorTable.LATERAL.createCall(SqlParserPos.ZERO, right.getNode()),
        JoinConditionType.NONE.symbol(SqlParserPos.ZERO),
        null);

    RelDataTypeFieldBuilder builder = new RelDataTypeFieldBuilder(new FieldInfoBuilder(planner.getTypeFactory()));
    builder.addAll(left.getType().getFieldList());
    builder.addAll(right.getType().getFieldList());
    RelDataType type = builder.build();

    Frame frame = new Frame(type, join, right.getAlias());
    stack.push(frame);

    return this;
  }

  public SqlNode buildAndProjectLast(List<String> pullupCols) {
    Frame frame = stack.pop();
    Frame lastTable = tableHistory.get(tableHistory.size()-1);
    SqlSelect select = new SqlSelectBuilder()
        .setFrom(frame.getNode())
        .setSelectList(ListUtils.union(rename(createSelectList(tableHistory.get(0), pullupCols.size()), pullupCols),
            createSelectList(lastTable, lastTable.type.getFieldCount())))
        .build();

    return select;
  }

  private List rename(List<SqlIdentifier> selectList, List<String> pullupCols) {
    return IntStream.range(0, selectList.size())
        .mapToObj(i-> SqlStdOperatorTable.AS.createCall(SqlParserPos.ZERO, selectList.get(i),
            new SqlIdentifier(pullupCols.get(i), SqlParserPos.ZERO)))
        .collect(Collectors.toList());
  }

  private List<SqlIdentifier> createSelectList(Frame frame, int count) {
    return IntStream.range(0, count)
        .mapToObj(i->
            //todo fix: alias has null check for no alias on subquery
            (frame.alias == null)
            ? new SqlIdentifier(List.of( frame.getType().getFieldList().get(i).getName()), SqlParserPos.ZERO )
            : new SqlIdentifier(List.of(frame.alias, frame.getType().getFieldList().get(i).getName()), SqlParserPos.ZERO )
        )
        .collect(Collectors.toList());
  }

  public String getLatestAlias() {
    return tableHistory.get(tableHistory.size()-1).alias;
  }

  public void scanNestedTable(List<String> currentPath) {
    SqrlPreparingTable relOptTable = planner.getCatalogReader().getSqrlTable(currentPath);
    String tableName = relOptTable.getQualifiedName().get(0);
    SqlIdentifier table = new SqlIdentifier(tableName, SqlParserPos.ZERO);

    SqlSelect select = new SqlSelectBuilder()
        .setFrom(table)
        .setSelectList(SqrlRelBuilder.shadow(relOptTable.getRowType()).getFieldList()
            .stream()
            .map(f-> (SqlNode)SqlStdOperatorTable.AS.createCall(SqlParserPos.ZERO,
                new SqlIdentifier(relOptTable.getInternalTable().getRowType().getFieldList().get(f.getIndex()).getName(), SqlParserPos.ZERO),
                new SqlIdentifier(f.getName(), SqlParserPos.ZERO)))
            .collect(Collectors.toList()))
        .build();

    String alias = "_t"+aliasInt.incrementAndGet();

    SqlCall aliasedCall = SqlStdOperatorTable.AS.createCall(SqlParserPos.ZERO, select, new SqlIdentifier(alias, SqlParserPos.ZERO));
    Frame frame = new Frame(SqrlRelBuilder.shadow(relOptTable.getRowType()), aliasedCall, alias);
    stack.push(frame);
    tableHistory.add(frame);

  }

  public void push(SqlNode result, RelDataType rowType) {
    Frame frame = new Frame(rowType, result, null);
    stack.push(frame);
    tableHistory.add(frame);

  }


  @Value
  public class Frame {
    RelDataType type;
    SqlNode node;
    String alias;
  }
}
