package ai.datasqrl.plan.calcite.hints;

import lombok.Getter;
import org.apache.calcite.rel.hint.HintPredicates;
import org.apache.calcite.rel.hint.HintStrategyTable;
import org.apache.calcite.sql.SqlHint;
import org.apache.calcite.sql.SqlHint.HintOptionFormat;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParserPos;

public class SqrlHintStrategyTable {

  public static final String DISTINCT_ON = "DISTINCT_ON";

  public static final String SELECT_DISTINCT = "SELECT_DISTINCT";

  public static final String TOP_N = "TOP_N";
  @Getter
  static HintStrategyTable hintStrategyTable = HintStrategyTable.builder()
      .hintStrategy(DISTINCT_ON, HintPredicates.PROJECT)
      .hintStrategy(SELECT_DISTINCT, HintPredicates.PROJECT)
      .hintStrategy(TOP_N, HintPredicates.PROJECT)
      .build();

  public static SqlHint createSelectDistinctHintNode(SqlNodeList columns, SqlParserPos pos) {
    return new SqlHint(pos, new SqlIdentifier(SELECT_DISTINCT, pos), columns, HintOptionFormat.ID_LIST);
  }

  public static SqlHint createTopNHintNode(SqlNodeList columns, SqlParserPos pos) {
    return new SqlHint(pos, new SqlIdentifier(TOP_N, pos), columns, HintOptionFormat.ID_LIST);
  }
}
