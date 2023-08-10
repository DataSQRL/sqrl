package com.datasqrl.calcite;

import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.SqlWriterConfig;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.SqlToRelConverter;

import java.util.function.UnaryOperator;

public class SqrlConfigurations {

    public static SqlValidator.Config sqlValidatorConfig = SqlValidator.Config.DEFAULT
        .withCallRewrite(true)
        .withIdentifierExpansion(false)
        .withColumnReferenceExpansion(true)
        .withTypeCoercionEnabled(true) //must be true to allow null literals
        .withLenientOperatorLookup(false)
        .withSqlConformance(SqrlConformance.INSTANCE);

    public static final SqlToRelConverter.Config sqlToRelConverterConfig = SqlToRelConverter
        .config()
        .withExpand(false)
        .withDecorrelationEnabled(false)
        .withTrimUnusedFields(false)
        /// STOPSHIP: 8/6/23  add hints when merged
        //.withHintStrategyTable(SqrlHintStrategyTable.getHintStrategyTable())
        ;

    public static final UnaryOperator<SqlWriterConfig> relToSql = c ->
        c.withAlwaysUseParentheses(false)
            .withSelectListItemsOnSeparateLines(false)
            .withUpdateSetListNewline(false)
            .withIndentation(1)
            .withQuoteAllIdentifiers(true)
            .withDialect(PostgresqlSqlDialect.DEFAULT)
            .withSelectFolding(null);

    public static final SqlParser.Config sqrlParserConfig = SqlParser.config()
        .withParserFactory(new SqrlParserFactory())
        .withCaseSensitive(true)
        .withQuoting(Quoting.DOUBLE_QUOTE)
        .withConformance(SqrlConformance.INSTANCE);



}
