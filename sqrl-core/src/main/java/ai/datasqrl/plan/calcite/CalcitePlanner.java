package ai.datasqrl.plan.calcite;

import ai.datasqrl.sql.calcite.NodeToSqlNodeConverter;
import ai.datasqrl.plan.SqrlRelBuilder;
import ai.datasqrl.parse.tree.Node;
import java.util.Properties;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.jdbc.SqrlCalciteSchema;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.SqrlSchema;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;

public class CalcitePlanner {

  private final RelOptCluster cluster;
  private final CalciteCatalogReader catalogReader;
  private final SqrlSchema sqrlSchema;
  private final SqrlCalciteSchema calciteSchema;
  private final JavaTypeFactoryImpl typeFactory;

  SqlValidator.Config validatorConfig = SqlValidator.Config.DEFAULT
      .withCallRewrite(true)
      .withIdentifierExpansion(true)
      .withColumnReferenceExpansion(true)
      .withTypeCoercionEnabled(false)
      .withLenientOperatorLookup(true)
      .withSqlConformance(SqlConformanceEnum.LENIENT)
      ;

  public CalcitePlanner() {
    this.typeFactory = new FlinkTypeFactory(new FlinkTypeSystem());
    this.cluster = CalciteTools.createHepCluster(typeFactory);
    this.sqrlSchema = new SqrlSchema();
    this.calciteSchema = new SqrlCalciteSchema(sqrlSchema);
    this.catalogReader = CalciteTools.getCalciteCatalogReader(calciteSchema);
  }

  public SqrlCalciteSchema getSchema() {
    return calciteSchema;
  }

  public void addTable(String name, org.apache.calcite.schema.Table table) {
    calciteSchema.add(name, table);
  }

  public SqrlRelBuilder createRelBuilder() {
    return new SqrlRelBuilder(null, cluster, catalogReader);
  }

  public SqlNode parse(Node node) {
    NodeToSqlNodeConverter converter = new NodeToSqlNodeConverter();
    SqlNode sqlNode = node.accept(converter, null);

    return sqlNode;
  }

  public RelNode plan(SqlNode sqlNode) {
    Properties props = new Properties();
    props.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), "false");

    SqlValidator validator = SqlValidatorUtil.newValidator(SqlStdOperatorTable.instance(),
        catalogReader, typeFactory,
        validatorConfig);

    SqlNode validated = validator.validate(sqlNode);

    SqlToRelConverter relConverter = new SqlToRelConverter(
        (rowType, queryString, schemaPath
            , viewPath) -> null,
        validator,
        catalogReader,
        cluster,
        StandardConvertletTable.INSTANCE,
        SqlToRelConverter.config().withExpand(false).withTrimUnusedFields(true).withCreateValuesRel(false));

    return relConverter.convertQuery(validated, false, true).rel;
  }

  public SqlValidator getValidator() {
    Properties props = new Properties();
    props.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), "false");

    SqlValidator validator = SqlValidatorUtil.newValidator(SqlStdOperatorTable.instance(),
        catalogReader, typeFactory,
        validatorConfig);

    return validator;
  }

  public SqlToRelConverter getSqlToRelConverter(SqlValidator validator) {
    SqlToRelConverter relConverter = new SqlToRelConverter(
        (rowType, queryString, schemaPath
            , viewPath)->null,
        validator,
        catalogReader,
        cluster,
        StandardConvertletTable.INSTANCE,
        SqlToRelConverter.config()
            .withTrimUnusedFields(true));
    return relConverter;
  }

  public void setView(String name, Table table) {
    this.calciteSchema.add(name, table);
  }
}
