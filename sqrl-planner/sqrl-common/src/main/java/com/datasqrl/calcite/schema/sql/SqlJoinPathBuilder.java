package com.datasqrl.calcite.schema.sql;

import com.datasqrl.calcite.CatalogReader;
import com.datasqrl.calcite.SqrlToSql.PullupColumn;
import com.datasqrl.calcite.schema.sql.SqlBuilders.SqlSelectBuilder;
import com.datasqrl.util.CalciteUtil.RelDataTypeFieldBuilder;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Stack;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Value;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory.FieldInfoBuilder;
import org.apache.calcite.sql.JoinConditionType;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlUserDefinedTableFunction;
import org.apache.commons.collections.ListUtils;

@AllArgsConstructor
public class SqlJoinPathBuilder {
  final CatalogReader catalogReader;
  @Getter
  final List<Frame> tableHistory = new ArrayList<>();
  final Stack<Frame> stack = new Stack<>();
  final AtomicInteger aliasInt = new AtomicInteger(0);

  public SqlJoinPathBuilder scanFunction(List<String> path, List<SqlNode> args) {
    Optional<SqlUserDefinedTableFunction> op = catalogReader.getTableFunction(path);
    if (op.isEmpty() && args.isEmpty()) {
      scanNestedTable(path);
      return this;
    }
    if (op.isEmpty()) {
      throw new RuntimeException(String.format("Could not find table: %s", path));
    }

    scanFunction(op.get(), args);
    return this;
  }

  public SqlJoinPathBuilder scanFunction(SqlUserDefinedTableFunction op,
      List<SqlNode> args) {
    RelDataType type = op.getFunction().getRowType(null, null);
    SqlCall call = op.createCall(SqlParserPos.ZERO, args);

    String alias = "_t"+aliasInt.incrementAndGet();
    call = SqlStdOperatorTable.COLLECTION_TABLE.createCall(SqlParserPos.ZERO, call);

    SqlCall aliasedCall = SqlStdOperatorTable.AS.createCall(SqlParserPos.ZERO, call, new SqlIdentifier(alias, SqlParserPos.ZERO));
    Frame frame = new Frame(false, type, aliasedCall, alias);
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

    RelDataTypeFieldBuilder builder = new RelDataTypeFieldBuilder(
        new FieldInfoBuilder(catalogReader.getTypeFactory()));
    builder.addAll(left.getType().getFieldList());
    builder.addAll(right.getType().getFieldList());
    RelDataType type = builder.build();

    Frame frame = new Frame(right.subquery, type, join, right.getAlias());
    stack.push(frame);

    return this;
  }

  public SqlNode build() {
    Frame frame = stack.pop();
    return frame.getNode();
  }
  public SqlNode buildAndProjectLast(List<PullupColumn> pullupCols) {
    Frame frame = stack.pop();
    Frame lastTable = tableHistory.get(tableHistory.size()-1);
    if (frame.isSubquery()) { //subquery
      return frame.getNode();
    }
    SqlSelectBuilder select = new SqlSelectBuilder()
        .setFrom(frame.getNode());
      select.setSelectList(ListUtils.union(
        rename(createSelectList(tableHistory.get(0), pullupCols.size()), pullupCols),
        createSelectList(lastTable, lastTable.type.getFieldCount())));
    return select.build();
  }

  private List rename(List<SqlIdentifier> selectList, List<PullupColumn> pullupCols) {
    return IntStream.range(0, selectList.size())
        .mapToObj(i-> SqlStdOperatorTable.AS.createCall(SqlParserPos.ZERO, selectList.get(i),
            new SqlIdentifier(pullupCols.get(i).getColumnName(), SqlParserPos.ZERO)))
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
    RelOptTable relOptTable = catalogReader.getTableFromPath(currentPath);
    if (relOptTable == null) {
      throw new RuntimeException("Could not find table: " + currentPath);
    }
    String tableName = relOptTable.getQualifiedName().get(0);
    SqlNode table = new SqlIdentifier(tableName, SqlParserPos.ZERO);

    String alias = "_t"+aliasInt.incrementAndGet();

    SqlCall aliasedCall = SqlStdOperatorTable.AS.createCall(SqlParserPos.ZERO, table, new SqlIdentifier(alias, SqlParserPos.ZERO));
    Frame frame = new Frame(false, relOptTable.getRowType(), aliasedCall, alias);
    stack.push(frame);
    tableHistory.add(frame);
  }

  @Value
  public class Frame {
    boolean subquery;
    RelDataType type;
    SqlNode node;
    String alias;
  }
}
