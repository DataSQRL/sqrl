package ai.dataeng.sqml.planner;

import ai.dataeng.sqml.schema.Namespace;
import ai.dataeng.sqml.tree.name.NamePath;
import java.util.Collections;
import java.util.Optional;
import java.util.Properties;
import lombok.SneakyThrows;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CachingSqrlSchema;
import org.apache.calcite.jdbc.SqrlTypeFactory;
import org.apache.calcite.prepare.SqrlCalciteCatalogReader;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SqrlSchema;
import org.apache.calcite.sql.FlattenTableNames;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqrlOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.util.ReflectiveSqlOperatorTable;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqrlValidator;

public class HeuristicPlannerImpl implements Planner {

  @SneakyThrows
  @Override
  public SqlNode parse(String sql) {
    SqlParser.Config parserConfig = SqlParser.config()
        .withParserFactory(SqlParser.config().parserFactory())
        .withLex(Lex.JAVA)
        .withConformance(SqlConformanceEnum.LENIENT)
        .withIdentifierMaxLength(256);
    SqlParser parser = SqlParser.create(sql, parserConfig);
    SqlNode sqlNode = parser.parseQuery();

    // Validate the initial AST
    FlattenTableNames.rewrite(sqlNode);

    return sqlNode;
  }

  @Override
  public SqlValidator getValidator(Optional<NamePath> context, Namespace namespace) {
    SqrlTypeFactory typeFactory = new SqrlTypeFactory();
    SqrlCalciteCatalogReader catalogReader = getCalciteCatalogReader(context, namespace, typeFactory);
    SqlValidator validator = getValidator(catalogReader, typeFactory, SqrlOperatorTable.instance());
    return validator;
  }

  private SqlValidator getValidator(SqrlCalciteCatalogReader catalogReader,
      RelDataTypeFactory typeFactory, ReflectiveSqlOperatorTable operatorTable) {
    SqlValidator.Config validatorConfig = SqlValidator.Config.DEFAULT
        .withCallRewrite(true)
        .withIdentifierExpansion(true)
        .withColumnReferenceExpansion(true)
        .withLenientOperatorLookup(true)
        .withSqlConformance(SqlConformanceEnum.LENIENT)
        ;

    SqlValidator validator = new SqrlValidator(
        operatorTable,
        catalogReader,
        typeFactory,
        validatorConfig);
    return validator;
  }

  private SqrlCalciteCatalogReader getCalciteCatalogReader(Optional<NamePath> context,
      Namespace namespace, SqrlTypeFactory typeFactory) {
    CalciteTableFactory calciteTableFactory = new CalciteTableFactory(context, namespace, new RelDataTypeFieldFactory(typeFactory));
    CachingSqrlSchema schema = new CachingSqrlSchema(new SqrlSchema(calciteTableFactory));

    // Configure and instantiate validator
    Properties props = new Properties();
    props.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), "false");
    CalciteConnectionConfig config = new CalciteConnectionConfigImpl(props);
    SqrlCalciteCatalogReader catalogReader = new SqrlCalciteCatalogReader(schema,
        Collections.singletonList(""),
        typeFactory, config);
    return catalogReader;
  }
}
