/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.table;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.io.tables.TableSource;
import com.datasqrl.plan.rules.LPAnalysis;
import java.util.Optional;
import lombok.Getter;
import org.apache.calcite.rel.type.RelDataType;

@Getter
public class CalciteTableFactory {

  private final TableIdFactory tableIdFactory;
  private final TableConverter tableConverter;

  public CalciteTableFactory(SqrlFramework framework) {
    this.tableIdFactory = new TableIdFactory(framework.getTableNameToIdMap());
    this.tableConverter = new TableConverter(framework.getTypeFactory(), framework.getQueryPlanner());
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

  public ProxyImportRelationalTable createProxyTable(RelDataType rootType, NamePath tablePath,
      ImportedRelationalTableImpl importedTable, TableType tableType, Optional<Integer> timestampIndex, PrimaryKey primaryKey) {
    Name proxyName = tableIdFactory.createTableId(tablePath.getLast());
    return new ProxyImportRelationalTable(
        proxyName,
        tablePath,
        timestampIndex.map(Timestamps::ofFixed).orElse(Timestamps.UNDEFINED),
        rootType, tableType, primaryKey,
        importedTable,
        TableStatistic.of(1000)
    );
  }

  public PhysicalRelationalTable createPhysicalRelTable(NamePath tablePath, LPAnalysis analyzedLP) {
    Name tableId = tableIdFactory.createTableId(tablePath.getLast());
    return new QueryRelationalTable(tableId, tablePath, analyzedLP);
  }



}
