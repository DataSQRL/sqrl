package ai.dataeng.sqml.parser.sqrl.calcite;

import ai.dataeng.sqml.parser.CalciteTools;
import ai.dataeng.sqml.parser.sqrl.LogicalDag;
import ai.dataeng.sqml.parser.sqrl.NodeToSqlNodeConverter;
import ai.dataeng.sqml.tree.Node;
import java.util.List;
import java.util.Properties;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CachingSqrlSchema2;
import org.apache.calcite.jdbc.SqrlTypeFactory;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable.ViewExpander;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.SqrlSchema2;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqrlRelBuilder;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;

public class CalcitePlanner {

  private final RelOptCluster cluster;
  private final CalciteCatalogReader catalogReader;
  private final SqrlSchema2 sqrlSchema;
  private final CachingSqrlSchema2 calciteSchema;

  public CalcitePlanner() {
    SqrlTypeFactory typeFactory = new SqrlTypeFactory();
    this.cluster = CalciteTools.createHepCluster(typeFactory);
    this.sqrlSchema = new SqrlSchema2();
    this.calciteSchema = new CachingSqrlSchema2(sqrlSchema);
    this.catalogReader = CalciteTools.getCalciteCatalogReader(calciteSchema);
  }

  public CachingSqrlSchema2 getSchema() {
    return calciteSchema;
  }

  public void addTable(String name, org.apache.calcite.schema.Table table) {
    calciteSchema.add(name, table);
  }

  public SqrlRelBuilder createRelBuilder() {
    return new SqrlRelBuilder(null, cluster, catalogReader);
  }


  public SqlNode parse(Node node) {
    SqrlTypeFactory typeFactory = new SqrlTypeFactory();
    Properties props = new Properties();
    props.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), "false");

    SqlValidator.Config validatorConfig = SqlValidator.Config.DEFAULT
        .withCallRewrite(true)
        .withIdentifierExpansion(true)
        .withColumnReferenceExpansion(true)
        .withLenientOperatorLookup(true)
        .withSqlConformance(SqlConformanceEnum.LENIENT);

    NodeToSqlNodeConverter converter = new NodeToSqlNodeConverter();
    SqlNode sqlNode = node.accept(converter, null);
//    SqlValidator validator = SqlValidatorUtil.newValidator(SqlStdOperatorTable.instance(),
//        catalogReader, typeFactory,
//        SqlValidator.Config.DEFAULT);
//
//    SqlNode validated = validator.validate(sqlNode);
    return sqlNode;
  }

    public RelNode plan(Node node) {
    SqrlTypeFactory typeFactory = new SqrlTypeFactory();
    Properties props = new Properties();
    props.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), "false");

    SqlValidator.Config validatorConfig = SqlValidator.Config.DEFAULT
        .withCallRewrite(true)
        .withIdentifierExpansion(true)
        .withColumnReferenceExpansion(true)
        .withLenientOperatorLookup(true)
        .withSqlConformance(SqlConformanceEnum.LENIENT)
        ;

    NodeToSqlNodeConverter converter = new NodeToSqlNodeConverter();
    SqlNode sqlNode = node.accept(converter, null);
    SqlValidator validator = SqlValidatorUtil.newValidator(SqlStdOperatorTable.instance(),
        catalogReader, typeFactory,
        SqlValidator.Config.DEFAULT);

    SqlNode validated = validator.validate(sqlNode);

    SqlToRelConverter relConverter = new SqlToRelConverter(
        (rowType, queryString, schemaPath
            , viewPath) -> null,
        validator,
        catalogReader,
        cluster,
        StandardConvertletTable.INSTANCE,
        SqlToRelConverter.config());

    return relConverter.convertQuery(validated, false, true).rel;
  }

  public SqlValidator getValidator() {
    SqrlTypeFactory typeFactory = new SqrlTypeFactory();
    Properties props = new Properties();
    props.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), "false");

    SqlValidator.Config validatorConfig = SqlValidator.Config.DEFAULT
        .withCallRewrite(true)
        .withIdentifierExpansion(true)
        .withColumnReferenceExpansion(true)
        .withLenientOperatorLookup(true)
        .withSqlConformance(SqlConformanceEnum.LENIENT)
        ;

    SqlValidator validator = SqlValidatorUtil.newValidator(SqlStdOperatorTable.instance(),
        catalogReader, typeFactory,
        SqlValidator.Config.DEFAULT);

    return validator;
  }

  public SqlToRelConverter getSqlToRelConverter(SqlValidator validator) {
    SqlToRelConverter relConverter = new SqlToRelConverter(
        new SqrlViewExpander(),
        validator,
        catalogReader,
        cluster,
        StandardConvertletTable.INSTANCE,
        SqlToRelConverter.config());
    return relConverter;
  }

  public class SqrlViewExpander implements ViewExpander {

    @Override
    public RelRoot expandView(RelDataType relDataType, String s, List<String> list,
        List<String> list1) {
      return null;
    }
  }
}
