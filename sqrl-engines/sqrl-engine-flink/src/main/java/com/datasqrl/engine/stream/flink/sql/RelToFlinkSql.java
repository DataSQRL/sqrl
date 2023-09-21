package com.datasqrl.engine.stream.flink.sql;

import com.datasqrl.VectorFunctions;
import com.datasqrl.calcite.type.TypeFactory;
import com.datasqrl.calcite.type.Vector;
import com.datasqrl.engine.stream.flink.sql.model.QueryPipelineItem;
import com.datasqrl.flink.FlinkConverter;
import com.datasqrl.function.StdVectorLibraryImpl;
import java.util.List;
import java.util.Optional;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.calcite.rel.RelNode;
import com.datasqrl.engine.stream.flink.sql.calcite.FlinkDialect;
import org.apache.calcite.rel.rel2sql.FlinkRelToSqlConverter;
import org.apache.calcite.rel.rel2sql.FlinkRelToSqlConverter.QueryType;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlUnresolvedFunction;
import org.apache.calcite.sql.SqlWriterConfig;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.tools.RelBuilder;

public class RelToFlinkSql {
  public static final UnaryOperator<SqlWriterConfig> transform = c ->
      c.withAlwaysUseParentheses(false)
          .withSelectListItemsOnSeparateLines(false)
          .withUpdateSetListNewline(false)
          .withIndentation(1)
          .withQuoteAllIdentifiers(true)
          .withDialect(PostgresqlSqlDialect.DEFAULT)
          .withSelectFolding(null);

  public static String convertToString(RelNode optimizedNode) {
    return convertToSqlNode(optimizedNode).toSqlString(
            c -> transform.apply(c.withDialect(FlinkDialect.DEFAULT)))
        .getSql();
  }

  public static String convertToString(SqlNode sqlNode) {
    return sqlNode.toSqlString(
            c -> transform.apply(c.withDialect(FlinkDialect.DEFAULT)))
        .getSql().replaceAll("\"", "`");
  }

  public static SqlNode convertToSqlNode(RelNode optimizedNode) {
    RelToSqlConverter converter = new RelToSqlConverter(FlinkDialect.DEFAULT);
    final SqlNode sqlNode = converter.visitRoot(optimizedNode).asStatement();
    return sqlNode;
  }

  public static String convertToSql(FlinkRelToSqlConverter converter, RelNode optimizedNode) {
    //add Casts
    List<Integer> toConvert = optimizedNode.getRowType().getFieldList().stream()
        .filter(f->f.getType() instanceof Vector)
        .map(RelDataTypeField::getIndex)
        .collect(Collectors.toList());
    if (!toConvert.isEmpty()) {

      RelBuilder relBuilder = new RelBuilder(null, optimizedNode.getCluster(), null){};
      relBuilder.push(optimizedNode);
      List<RexNode> collect = IntStream.range(0, optimizedNode.getRowType().getFieldCount())
          .mapToObj(i -> toConvert.contains(i) ?
              relBuilder.call(op("VecToDouble"),
                  relBuilder.field(i))
              :relBuilder.field(i))
          .collect(Collectors.toList());

      optimizedNode = relBuilder.project(collect, optimizedNode.getRowType().getFieldNames())
          .build();
    }

    final SqlNode sqlNode = converter.visitRoot(optimizedNode).asStatement();
    QueryPipelineItem query = converter.getOrCreate(QueryType.ROOT, sqlNode, optimizedNode, null);

    return query.getTableName();
  }

  public static SqlUnresolvedFunction op(String name) {
    return new SqlUnresolvedFunction(new SqlIdentifier(name, SqlParserPos.ZERO),
        null, null, null,
        null, SqlFunctionCategory.USER_DEFINED_FUNCTION);
  }
}
