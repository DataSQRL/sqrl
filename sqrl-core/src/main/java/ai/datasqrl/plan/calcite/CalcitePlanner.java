package ai.datasqrl.plan.calcite;

import ai.datasqrl.plan.nodes.SqrlRelBuilder;
import java.util.Properties;
import lombok.Getter;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.AbstractSqrlSchema;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;

@Getter
public class CalcitePlanner {

  private final RelOptCluster cluster;
  private final CalciteCatalogReader catalogReader;
  private final AbstractSqrlSchema sqrlSchema;
  private final org.apache.calcite.jdbc.SqrlCalciteSchema calciteSchema;
  private final JavaTypeFactoryImpl typeFactory;

  public static SqlValidator.Config validatorConfig = SqlValidator.Config.DEFAULT
      .withCallRewrite(true)
      .withIdentifierExpansion(true)
      .withColumnReferenceExpansion(true)
      .withTypeCoercionEnabled(false)
      .withLenientOperatorLookup(true)
      .withSqlConformance(SqlConformanceEnum.LENIENT);

  public CalcitePlanner(AbstractSqrlSchema sqrlSchema) {
    this.sqrlSchema = sqrlSchema;
    this.typeFactory = new FlinkTypeFactory(new FlinkTypeSystem());
    this.cluster = CalciteTools.createHepCluster(typeFactory);
    this.calciteSchema = new org.apache.calcite.jdbc.SqrlCalciteSchema(sqrlSchema);
    this.catalogReader = CalciteTools.getCalciteCatalogReader(calciteSchema);
  }

  public org.apache.calcite.jdbc.SqrlCalciteSchema getSchema() {
    return calciteSchema;
  }

  public SqrlRelBuilder createRelBuilder() {
    return new SqrlRelBuilder(null, cluster, catalogReader);
  }

  public RelNode plan(SqlNode sqlNode, SqlValidator validator) {
    Properties props = new Properties();
    props.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), "false");

    SqlToRelConverter relConverter = new SqlToRelConverter(
        (rowType, queryString, schemaPath
            , viewPath) -> null,
        validator,
        catalogReader,
        cluster,
        StandardConvertletTable.INSTANCE,
        SqlToRelConverter.config().withExpand(false).withTrimUnusedFields(true)
            .withCreateValuesRel(false));

    return relConverter.convertQuery(sqlNode, false, true).rel;
  }

  public SqlValidator createValidator() {
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
            , viewPath) -> null,
        validator,
        catalogReader,
        cluster,
        StandardConvertletTable.INSTANCE,
        SqlToRelConverter.config()
            .withTrimUnusedFields(true));
    return relConverter;
  }
}
