/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.loaders;

import com.datasqrl.function.builtin.time.FlinkFnc;
import com.datasqrl.io.tables.TableSource;
import com.datasqrl.name.Name;
import java.util.Optional;

public interface LoaderContext extends ExporterContext {

  public void addFunction(FlinkFnc flinkFnc);

  Name registerTable(TableSource table, Optional<Name> alias);
}
