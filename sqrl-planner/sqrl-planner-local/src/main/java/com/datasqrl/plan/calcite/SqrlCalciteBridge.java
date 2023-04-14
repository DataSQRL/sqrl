/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.calcite;

/**
 * Bridges calcite with sqrl planning operations
 */
public interface SqrlCalciteBridge {

  org.apache.calcite.schema.Table getTable(String tableName);
}
