package com.datasqrl.calcite;

import lombok.Getter;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

import java.util.Properties;

@Getter
public class SqrlFramework {
  private final CatalogReader catalogReader;
  private final TypeFactory typeFactory;
  private final SqrlTypeSystem dataTypeSystem;
  private final OperatorTable sqrlOperatorTable;
  private final SqrlSchema schema;
  private final QueryPlanner queryPlanner;
  private final CalciteSchema rootSchema;

  public SqrlFramework() {
    this.schema = new SqrlSchema();

    //todo: service load
    this.typeFactory = new TypeFactory();
    this.dataTypeSystem = new SqrlTypeSystem();

    Properties info = new Properties();
    info.setProperty("caseSensitive", "false");
    CalciteConnectionConfigImpl config = new CalciteConnectionConfigImpl(info);
    this.rootSchema = CalciteSchema.createRootSchema(false);
    CalciteSchema sqrlSchema = rootSchema.add("SQRL", schema);
    this.catalogReader = new CatalogReader(sqrlSchema, typeFactory, config);
    this.sqrlOperatorTable = new OperatorTable(catalogReader, SqlStdOperatorTable.instance());

    this.queryPlanner = new QueryPlanner(catalogReader, sqrlOperatorTable, typeFactory);
  }
}
