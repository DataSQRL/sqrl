package com.datasqrl.calcite;

import com.datasqrl.calcite.type.TypeFactory;
import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.google.inject.Inject;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import lombok.Getter;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.jdbc.SqrlSchema;
import org.apache.calcite.rel.hint.HintStrategyTable;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.internal.TableEnvExtractor;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;
import org.apache.flink.table.planner.calcite.RexFactory;
import org.apache.flink.table.planner.catalog.FunctionCatalogOperatorTable;

@Getter
public class SqrlFramework {

  private final CatalogReader catalogReader;
  private final TypeFactory typeFactory;
  private final OperatorTable sqrlOperatorTable;
  private final SqrlSchema schema;
  private final HintStrategyTable hintStrategyTable;
  private final NameCanonicalizer nameCanonicalizer;
  private final FunctionCatalog flinkFunctionCatalog; // Flink's function catalog (for udfs)
  private QueryPlanner queryPlanner;
  private final RelMetadataProvider relMetadataProvider;

  public SqrlFramework() {
    this(
        null,
        HintStrategyTable.builder().build(),
        NameCanonicalizer.SYSTEM,
        new SqrlSchema(new TypeFactory(), NameCanonicalizer.SYSTEM));
  }

  @Inject
  public SqrlFramework(
      RelMetadataProvider relMetadataProvider,
      HintStrategyTable hintStrategyTable,
      NameCanonicalizer nameCanonicalizer,
      SqrlSchema schema) {
    this(relMetadataProvider, hintStrategyTable, nameCanonicalizer, schema, Optional.empty());
  }

  public SqrlFramework(
      RelMetadataProvider relMetadataProvider,
      HintStrategyTable hintStrategyTable,
      NameCanonicalizer nameCanonicalizer,
      SqrlSchema schema,
      Optional<QueryPlanner> planner) {
    this.hintStrategyTable = hintStrategyTable;
    this.typeFactory = new TypeFactory();
    this.schema = schema;
    // Add type aliases
    schema.add("String", t -> typeFactory.createSqlType(SqlTypeName.VARCHAR));
    // Int -> Integer
    schema.add("Int", t -> typeFactory.createSqlType(SqlTypeName.INTEGER));

    this.relMetadataProvider = relMetadataProvider;

    Properties info = new Properties();
    info.setProperty("caseSensitive", "false");
    CalciteConnectionConfigImpl config = new CalciteConnectionConfigImpl(info);

    this.nameCanonicalizer = nameCanonicalizer;
    this.catalogReader = new CatalogReader(schema, typeFactory, config);
    TableEnvironmentImpl tableEnvironment =
        TableEnvironmentImpl.create(Configuration.fromMap(Map.of()));
    this.flinkFunctionCatalog = TableEnvExtractor.getFunctionCatalog(tableEnvironment);

    SqlOperatorTable chain =
        SqlOperatorTables.chain(
            new FunctionCatalogOperatorTable(
                flinkFunctionCatalog,
                tableEnvironment.getCatalogManager().getDataTypeFactory(),
                typeFactory,
                new RexFactory(
                    new FlinkTypeFactory(getClass().getClassLoader(), FlinkTypeSystem.INSTANCE),
                    () -> null,
                    () -> null,
                    (x) -> null)),
            new OperatorTable(catalogReader, schema));
    this.sqrlOperatorTable = new OperatorTable(schema, chain);

    this.queryPlanner = planner.orElseGet(this::resetPlanner);
  }

  public QueryPlanner resetPlanner() {
    this.queryPlanner = new QueryPlanner(this);
    return this.queryPlanner;
  }
}
