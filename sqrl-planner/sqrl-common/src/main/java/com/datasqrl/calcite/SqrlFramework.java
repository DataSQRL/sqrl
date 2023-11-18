package com.datasqrl.calcite;

import com.datasqrl.calcite.type.ForeignType;
import com.datasqrl.calcite.type.TypeFactory;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NameCanonicalizer;

import com.datasqrl.util.ServiceLoaderDiscovery;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Getter;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.jdbc.SqrlSchema;
import org.apache.calcite.rel.hint.HintStrategyTable;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

import java.util.Properties;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.implicit.TypeCoercionFactory;

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
    this.typeFactory = new TypeFactory();
    this.schema = new SqrlSchema(this, typeFactory);
    //Add type aliases
    this.schema.add("String", t->typeFactory.createSqlType(SqlTypeName.VARCHAR));
    //Int -> Integer
    this.schema.add("Int", t->typeFactory.createSqlType(SqlTypeName.INTEGER));

    ServiceLoaderDiscovery.getAll(ForeignType.class)
            .forEach(f->this.schema.add(f.getName(), t->f));

    typeFactory.getTypes().stream()
        .filter(f->f instanceof RelProtoDataType)
        .forEach(t->schema.add(t.getFullTypeString(), (RelProtoDataType) t));

    this.relMetadataProvider = relMetadataProvider;

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
