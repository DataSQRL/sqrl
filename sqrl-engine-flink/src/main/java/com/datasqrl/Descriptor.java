/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl;

import com.datasqrl.io.DataSystemConnector;
import com.datasqrl.io.tables.TableConfig;
import com.datasqrl.plan.global.OptimizedDAG;
import com.datasqrl.plan.global.OptimizedDAG.WriteSink;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;

public interface Descriptor<T extends DataSystemConnector> {

  TableDescriptor create(String name, Schema schema, T connector,
      TableConfig configuration);

}
