package com.datasqrl.calcite;

import java.util.function.UnaryOperator;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.SqlWriterConfig;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.SqlToRelConverter;

public class SqrlConfigurations {

  public static final UnaryOperator<SqlWriterConfig> sqlToPostgresString = c ->
      c.withAlwaysUseParentheses(false)
      .withSelectListItemsOnSeparateLines(false)
      .withUpdateSetListNewline(false)
      .withIndentation(1)
      .withQuoteAllIdentifiers(true)
      .withDialect(PostgresqlSqlDialect.DEFAULT)
      .withSelectFolding(null);

  public static final SqlToRelConverter.Config sqlToRelConverterConfig = SqlToRelConverter
      .config()
      .withExpand(false)
      .withDecorrelationEnabled(false)
      .withTrimUnusedFields(false);

  public static SqlValidator.Config sqlValidatorConfig = SqlValidator.Config.DEFAULT
      .withCallRewrite(true)
      .withIdentifierExpansion(false)
      .withColumnReferenceExpansion(true)
      .withTypeCoercionEnabled(true) //must be true to allow null literals
      .withLenientOperatorLookup(false)
      .withSqlConformance(SqrlConformance.INSTANCE);
  public static SqlParser.Config calciteParserConfig = SqlParser.config()
      .withCaseSensitive(true)
      .withConformance(SqlConformanceEnum.DEFAULT)
      .withQuoting(Quoting.DOUBLE_QUOTE);
}
