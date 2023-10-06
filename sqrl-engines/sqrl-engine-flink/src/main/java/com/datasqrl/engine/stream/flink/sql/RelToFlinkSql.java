package com.datasqrl.engine.stream.flink.sql;

import com.datasqrl.calcite.type.ForeignType;
import com.datasqrl.engine.stream.flink.sql.calcite.FlinkDialect;
import com.datasqrl.engine.stream.flink.sql.model.QueryPipelineItem;
import java.util.List;
import java.util.Optional;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rel2sql.FlinkRelToSqlConverter;
import org.apache.calcite.rel.rel2sql.FlinkRelToSqlConverter.QueryType;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriterConfig;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;
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
    boolean requiresConversion = optimizedNode.getRowType().getFieldList().stream()
        .filter(field->field.getType() instanceof ForeignType)
        .anyMatch(field -> ((ForeignType)field.getType()).getDowncastFunction().isPresent());

    if (requiresConversion) {
      RelBuilder relBuilder = new RelBuilder(null, optimizedNode.getCluster(), null){};
      relBuilder.push(optimizedNode);
      List<RexNode> collect = optimizedNode.getRowType().getFieldList().stream()
          .map(field -> {
            Optional<SqlFunction> downcastFunction = (field.getType() instanceof ForeignType)
                ? ((ForeignType) field.getType()).getDowncastFunction()
                : Optional.empty();

            return downcastFunction
                .map(f->relBuilder.call(f, relBuilder.field(field.getIndex())))
                .orElse(relBuilder.field(field.getIndex()));
          })
          .collect(Collectors.toList());
      optimizedNode = relBuilder.project(collect, optimizedNode.getRowType().getFieldNames())
          .build();
    }

    final SqlNode sqlNode = converter.visitRoot(optimizedNode).asStatement();
    QueryPipelineItem query = converter.getOrCreate(QueryType.ROOT, sqlNode, optimizedNode, null);

    return query.getTableName();
  }
}
