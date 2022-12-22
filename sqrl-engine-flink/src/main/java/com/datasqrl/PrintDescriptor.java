/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl;

import com.datasqrl.io.impl.print.PrintDataSystem;
import com.datasqrl.io.impl.print.PrintDataSystem.Connector;
import com.datasqrl.io.tables.TableConfig;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;

public class PrintDescriptor implements Descriptor<PrintDataSystem.Connector> {

  @Override
  public TableDescriptor create(String name, Schema schema, Connector connector, TableConfig configuration) {
    return TableDescriptor.forConnector("print")
        .schema(schema)
        .option("print-identifier", name)
        .build();
  }
}
