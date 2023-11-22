/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.table;

import static com.datasqrl.plan.table.TimestampUtil.getTimestampInference;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.io.tables.TableSource;
import com.datasqrl.plan.rules.LPAnalysis;
import com.datasqrl.schema.UniversalTable;
import com.google.inject.Inject;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.NonNull;
import org.apache.calcite.rel.type.RelDataType;

@Getter
public class CalciteTableFactory {

  private final TableIdFactory tableIdFactory;
  private final TableConverter tableConverter;

  @Inject
  public CalciteTableFactory(SqrlFramework framework) {
    this.tableIdFactory = new TableIdFactory(framework.getTableNameToIdMap());
    this.tableConverter = new TableConverter(framework.getTypeFactory(), framework.getNameCanonicalizer());
  }
  public CalciteTableFactory(TableIdFactory tableIdFactory, TableConverter tableConverter) {
    this.tableIdFactory = tableIdFactory;
    this.tableConverter = tableConverter;
  }

  public ImportedRelationalTableImpl createImportedTable(RelDataType rootType,
      TableSource tableSource, Name tableName) {
    Name importName = tableIdFactory.createTableId(tableName);
    return new ImportedRelationalTableImpl(importName, rootType, tableSource);
  }

  public ProxyImportRelationalTable createProxyTable(RelDataType rootType, UniversalTable rootTable,
      ImportedRelationalTableImpl importedTable) {
    Name proxyName = tableIdFactory.createTableId(rootTable.getName());
    TimestampInference tsInference = getTimestampInference(rootTable);
    return new ProxyImportRelationalTable(
        proxyName,
        rootTable.getName(),
        tsInference,
        rootType,
        importedTable,
        TableStatistic.of(1000)
    );
  }

  public PhysicalRelationalTable createPhysicalRelTable(Name name, LPAnalysis analyzedLP) {
    Name tableId = tableIdFactory.createTableId(name);
    return new QueryRelationalTable(tableId, name, analyzedLP);
  }

  public ScriptRelationalTable createScriptTable(@NonNull UniversalTable table,
      ScriptRelationalTable parent) {
    Name tableId = tableIdFactory.createTableId(table.getName());
    RelDataType rowType = tableConverter.tableToDataType(table);
    return LogicalNestedTable.of(
        tableId,
        rowType,
        parent,
        table.getName().getCanonical(),
        tableConverter.typeFactory);
  }

  public Map<NamePath, ScriptRelationalTable> createScriptTables(UniversalTable builder,
      ScriptRelationalTable parent) {
    Map<NamePath, ScriptRelationalTable> createdTables = new LinkedHashMap<>();
    //Special case: at the root we just add to the map
    final ScriptRelationalTable nextParent;
    if (builder.getParent().isEmpty()) {
      assert parent instanceof PhysicalRelationalTable;
      nextParent = parent;
    } else {
      nextParent = createScriptTable(builder, parent);
    }
    createdTables.put(builder.getPath(), nextParent);


    Map<NamePath, ScriptRelationalTable> childTables = builder.getNestedTables().values().stream()
        .flatMap(c -> createScriptTables(c, nextParent).entrySet().stream())
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    createdTables.putAll(childTables);
    return createdTables;
  }
}
