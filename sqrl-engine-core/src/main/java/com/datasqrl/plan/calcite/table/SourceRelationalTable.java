/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.calcite.table;

import com.datasqrl.engine.stream.plan.TableRegistration;

public interface SourceRelationalTable {

  String getNameId();

  <R, C> R accept(TableRegistration<R, C> tableRegistration, C context);
}
