package com.datasqrl.v2;

import com.datasqrl.v2.tables.SqrlTableFunction;
import java.util.List;
import java.util.Properties;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.TableFunction;
import org.apache.calcite.sql.SqlOperatorTable;

public class SqrlFunctionCatalog {

  CalciteSchema schema;
  SchemaPlus schemaPlus;
  CalciteCatalogReader catalogReader;

  public SqrlFunctionCatalog(RelDataTypeFactory typeFactory) {
    schema = CalciteSchema.createRootSchema(false, false);
    schemaPlus = schema.plus();
    Properties info = new Properties();
    info.setProperty("caseSensitive", "false");
    CalciteConnectionConfigImpl config = new CalciteConnectionConfigImpl(info);
    this.catalogReader = new CalciteCatalogReader(schema, List.of(), typeFactory, config);
  }

  public void addFunction(SqrlTableFunction function) {
    schemaPlus.add(function.getFunctionCatalogName(), function);
  }

  public void addFunction(String name, TableFunction function) {
    schemaPlus.add(name, function);
  }

  public SqlOperatorTable getOperatorTable() {
    return catalogReader;
  }
}
