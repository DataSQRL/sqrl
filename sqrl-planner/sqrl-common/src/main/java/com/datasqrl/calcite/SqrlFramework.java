package com.datasqrl.calcite;

import com.datasqrl.calcite.type.TypeFactory;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NameCanonicalizer;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Getter;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.jdbc.SqrlSchema;
import org.apache.calcite.rel.hint.HintStrategyTable;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

import java.util.Properties;

@Getter
public class SqrlFramework {

  private final CatalogReader catalogReader;
  private final TypeFactory typeFactory;
  private final OperatorTable sqrlOperatorTable;
  private final SqrlSchema schema;
  private final HintStrategyTable hintStrategyTable;
  private final NameCanonicalizer nameCanonicalizer;
  private QueryPlanner queryPlanner;
  private final RelMetadataProvider relMetadataProvider;
  private final AtomicInteger uniqueCompilerId = new AtomicInteger(0);
  private final AtomicInteger uniquePkId = new AtomicInteger(0);
  private final AtomicInteger uniqueMacroInt = new AtomicInteger(0);
  private final Map<Name, AtomicInteger> tableNameToIdMap = new HashMap<>();

  public SqrlFramework() {
    this(null, HintStrategyTable.builder().build(), NameCanonicalizer.SYSTEM);
  }

  public SqrlFramework(RelMetadataProvider relMetadataProvider, HintStrategyTable hintStrategyTable,
      NameCanonicalizer nameCanonicalizer) {
    this.hintStrategyTable = hintStrategyTable;
    this.schema = new SqrlSchema(this);
    this.relMetadataProvider = relMetadataProvider;

    this.typeFactory = new TypeFactory();

    Properties info = new Properties();
    info.setProperty("caseSensitive", "false");
    CalciteConnectionConfigImpl config = new CalciteConnectionConfigImpl(info);

    this.nameCanonicalizer = nameCanonicalizer;
    this.catalogReader = new CatalogReader(schema, typeFactory, config);
    this.sqrlOperatorTable = new OperatorTable(nameCanonicalizer, catalogReader, SqlStdOperatorTable.instance());
    this.queryPlanner = resetPlanner();
  }

  public QueryPlanner resetPlanner() {
    this.queryPlanner = new QueryPlanner(this);
    return this.queryPlanner;
  }
}
