//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package org.apache.calcite.prepare;

import java.io.Reader;
import java.util.List;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCostFactory;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.RelOptTable.ViewExpander;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.metadata.CachingRelMetadataProvider;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexExecutor;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParser.Config;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.sql2rel.SqlRexConvertletTable;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.ValidationException;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.apache.flink.calcite.shaded.com.google.common.collect.ImmutableList;
import org.apache.flink.calcite.shaded.com.google.common.collect.UnmodifiableIterator;

public class PlannerImpl2 implements Planner, ViewExpander {
  private final SqlOperatorTable operatorTable;
  private final ImmutableList<Program> programs;
  private final RelOptCostFactory costFactory;
  private final Context context;
  private final CalciteConnectionConfig connectionConfig;
  private final ImmutableList<RelTraitDef> traitDefs;
  private final Config parserConfig;
  private final org.apache.calcite.sql.validate.SqlValidator.Config sqlValidatorConfig;
  private final org.apache.calcite.sql2rel.SqlToRelConverter.Config sqlToRelConverterConfig;
  private final SqlRexConvertletTable convertletTable;
  private PlannerImpl2.State state;
  private boolean open;
  private SchemaPlus defaultSchema;
  private JavaTypeFactory typeFactory;
  private RelOptPlanner planner;
  private RexExecutor executor;
  private SqlValidator validator;
  private SqlNode validatedSqlNode;
  private RelRoot root;

  public PlannerImpl2(FrameworkConfig config) {
    this.costFactory = config.getCostFactory();
    this.defaultSchema = config.getDefaultSchema();
    this.operatorTable = config.getOperatorTable();
    this.programs = config.getPrograms();
    this.parserConfig = config.getParserConfig();
    this.sqlValidatorConfig = config.getSqlValidatorConfig();
    this.sqlToRelConverterConfig = config.getSqlToRelConverterConfig();
    this.state = PlannerImpl2.State.STATE_0_CLOSED;
    this.traitDefs = config.getTraitDefs();
    this.convertletTable = config.getConvertletTable();
    this.executor = config.getExecutor();
    this.context = config.getContext();
    this.connectionConfig = this.connConfig();
    this.reset();
  }

  private CalciteConnectionConfig connConfig() {
    CalciteConnectionConfigImpl config = (CalciteConnectionConfigImpl)Util.first(this.context.unwrap(CalciteConnectionConfigImpl.class), CalciteConnectionConfig.DEFAULT);
    if (!config.isSet(CalciteConnectionProperty.CASE_SENSITIVE)) {
      config = config.set(CalciteConnectionProperty.CASE_SENSITIVE, String.valueOf(this.parserConfig.caseSensitive()));
    }

    if (!config.isSet(CalciteConnectionProperty.CONFORMANCE)) {
      config = config.set(CalciteConnectionProperty.CONFORMANCE, String.valueOf(this.parserConfig.conformance()));
    }

    return config;
  }

  private void ensure(PlannerImpl2.State state) {
    if (state != this.state) {
      if (state.ordinal() < this.state.ordinal()) {
        throw new IllegalArgumentException("cannot move to " + state + " from " + this.state);
      } else {
        state.from(this);
      }
    }
  }

  public RelTraitSet getEmptyTraitSet() {
    return this.planner.emptyTraitSet();
  }

  public void close() {
    this.open = false;
    this.typeFactory = null;
    this.state = PlannerImpl2.State.STATE_0_CLOSED;
  }

  public void reset() {
    this.ensure(PlannerImpl2.State.STATE_0_CLOSED);
    this.open = true;
    this.state = PlannerImpl2.State.STATE_1_RESET;
  }

  private void ready() {
    switch(this.state) {
    case STATE_0_CLOSED:
      this.reset();
    }

    this.ensure(PlannerImpl2.State.STATE_1_RESET);
    RelDataTypeSystem typeSystem = (RelDataTypeSystem)this.connectionConfig.typeSystem(RelDataTypeSystem.class, RelDataTypeSystem.DEFAULT);
    this.typeFactory = new JavaTypeFactoryImpl(typeSystem);
    this.planner = new VolcanoPlanner(this.costFactory, this.context);
    RelOptUtil.registerDefaultRules(this.planner, this.connectionConfig.materializationsEnabled(), (Boolean)Hook.ENABLE_BINDABLE.get(false));
    this.planner.setExecutor(this.executor);
    this.state = PlannerImpl2.State.STATE_2_READY;
    if (this.traitDefs == null) {
      this.planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
      if ((Boolean)CalciteSystemProperty.ENABLE_COLLATION_TRAIT.value()) {
        this.planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);
      }
    } else {
      UnmodifiableIterator var2 = this.traitDefs.iterator();

      while(var2.hasNext()) {
        RelTraitDef def = (RelTraitDef)var2.next();
        this.planner.addRelTraitDef(def);
      }
    }

  }

  public SqlNode parse(Reader reader) throws SqlParseException {
    switch(this.state) {
    case STATE_0_CLOSED:
    case STATE_1_RESET:
      this.ready();
    default:
      this.ensure(PlannerImpl2.State.STATE_2_READY);
      SqlParser parser = SqlParser.create(reader, this.parserConfig);
      SqlNode sqlNode = parser.parseStmt();
      this.state = PlannerImpl2.State.STATE_3_PARSED;
      return sqlNode;
    }
  }

  public SqlNode validate(SqlNode sqlNode) throws ValidationException {
    this.ensure(PlannerImpl2.State.STATE_3_PARSED);
    this.validator = this.createSqlValidator(this.createCatalogReader());

    try {
      this.validatedSqlNode = this.validator.validate(sqlNode);
    } catch (RuntimeException var3) {
      throw new ValidationException(var3);
    }

    this.state = PlannerImpl2.State.STATE_4_VALIDATED;
    return this.validatedSqlNode;
  }

  public Pair<SqlNode, RelDataType> validateAndGetType(SqlNode sqlNode) throws ValidationException {
    SqlNode validatedNode = this.validate(sqlNode);
    RelDataType type = this.validator.getValidatedNodeType(validatedNode);
    return Pair.of(validatedNode, type);
  }

  public final RelNode convert(SqlNode sql) {
    return this.rel(sql).rel;
  }

  public RelRoot rel(SqlNode sql) {
    this.ensure(PlannerImpl2.State.STATE_4_VALIDATED);

    assert this.validatedSqlNode != null;

    RexBuilder rexBuilder = this.createRexBuilder();
    RelOptCluster cluster = RelOptCluster.create(this.planner, rexBuilder);
    org.apache.calcite.sql2rel.SqlToRelConverter.Config config = this.sqlToRelConverterConfig.withTrimUnusedFields(false);
    SqlToRelConverter sqlToRelConverter = new SqlToRelConverter(this, this.validator, this.createCatalogReader(), cluster, this.convertletTable, config);
    this.root = sqlToRelConverter.convertQuery(this.validatedSqlNode, false, true);
    this.root = this.root.withRel(
//        sqlToRelConverter.flattenTypes(
            this.root.rel
//            , true)
    );
    RelBuilder relBuilder = config.getRelBuilderFactory().create(cluster, (RelOptSchema)null);
    this.root = this.root.withRel(RelDecorrelator.decorrelateQuery(this.root.rel, relBuilder));
    this.state = PlannerImpl2.State.STATE_5_CONVERTED;
    return this.root;
  }

  public RelRoot expandView(RelDataType rowType, String queryString, List<String> schemaPath, List<String> viewPath) {
    if (this.planner == null) {
      this.ready();
    }

    SqlParser parser = SqlParser.create(queryString, this.parserConfig);

    SqlNode sqlNode;
    try {
      sqlNode = parser.parseQuery();
    } catch (SqlParseException var16) {
      throw new RuntimeException("parse failed", var16);
    }

    CalciteCatalogReader catalogReader = this.createCatalogReader().withSchemaPath(schemaPath);
    SqlValidator validator = this.createSqlValidator(catalogReader);
    RexBuilder rexBuilder = this.createRexBuilder();
    RelOptCluster cluster = RelOptCluster.create(this.planner, rexBuilder);
    org.apache.calcite.sql2rel.SqlToRelConverter.Config config = this.sqlToRelConverterConfig.withTrimUnusedFields(false);
    SqlToRelConverter sqlToRelConverter = new SqlToRelConverter(this, validator, catalogReader, cluster, this.convertletTable, config);
    RelRoot root = sqlToRelConverter.convertQuery(sqlNode, true, false);
    RelRoot root2 = root.withRel(sqlToRelConverter.flattenTypes(root.rel, true));
    RelBuilder relBuilder = config.getRelBuilderFactory().create(cluster, (RelOptSchema)null);
    return root2.withRel(RelDecorrelator.decorrelateQuery(root.rel, relBuilder));
  }

  private CalciteCatalogReader createCatalogReader() {
    SchemaPlus rootSchema = rootSchema(this.defaultSchema);
    return new CalciteCatalogReader(CalciteSchema.from(rootSchema), CalciteSchema.from(this.defaultSchema).path((String)null), this.typeFactory, this.connectionConfig);
  }

  private SqlValidator createSqlValidator(CalciteCatalogReader catalogReader) {
    SqlOperatorTable opTab = SqlOperatorTables.chain(new SqlOperatorTable[]{this.operatorTable, catalogReader});
    return new CalciteSqlValidator(opTab, catalogReader, this.typeFactory, this.sqlValidatorConfig.withDefaultNullCollation(this.connectionConfig.defaultNullCollation()).withLenientOperatorLookup(this.connectionConfig.lenientOperatorLookup()).withSqlConformance(this.connectionConfig.conformance()).withIdentifierExpansion(true));
  }

  private static SchemaPlus rootSchema(SchemaPlus schema) {
    while(schema.getParentSchema() != null) {
      schema = schema.getParentSchema();
    }

    return schema;
  }

  private RexBuilder createRexBuilder() {
    return new RexBuilder(this.typeFactory);
  }

  public JavaTypeFactory getTypeFactory() {
    return this.typeFactory;
  }

  public RelNode transform(int ruleSetIndex, RelTraitSet requiredOutputTraits, RelNode rel) {
    this.ensure(PlannerImpl2.State.STATE_5_CONVERTED);
    rel.getCluster().setMetadataProvider(new CachingRelMetadataProvider(rel.getCluster().getMetadataProvider(), rel.getCluster().getPlanner()));
    Program program = (Program)this.programs.get(ruleSetIndex);
    return program.run(this.planner, rel, requiredOutputTraits, ImmutableList.of(), ImmutableList.of());
  }

  private static enum State {
    STATE_0_CLOSED {
      void from(PlannerImpl2 planner) {
        planner.close();
      }
    },
    STATE_1_RESET {
      void from(PlannerImpl2 planner) {
        planner.ensure(STATE_0_CLOSED);
        planner.reset();
      }
    },
    STATE_2_READY {
      void from(PlannerImpl2 planner) {
        STATE_1_RESET.from(planner);
        planner.ready();
      }
    },
    STATE_3_PARSED,
    STATE_4_VALIDATED,
    STATE_5_CONVERTED;

    private State() {
    }

    void from(PlannerImpl2 planner) {
      throw new IllegalArgumentException("cannot move from " + planner.state + " to " + this);
    }
  }

  /** @deprecated */
  @Deprecated
  public class ViewExpanderImpl implements ViewExpander {
    ViewExpanderImpl() {
    }

    public RelRoot expandView(RelDataType rowType, String queryString, List<String> schemaPath, List<String> viewPath) {
      return PlannerImpl2.this.expandView(rowType, queryString, schemaPath, viewPath);
    }
  }
}
