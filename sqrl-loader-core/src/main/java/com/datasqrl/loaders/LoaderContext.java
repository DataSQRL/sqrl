/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.loaders;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.function.builtin.time.FlinkFnc;
import com.datasqrl.io.tables.TableSource;
import com.datasqrl.name.Name;

import java.nio.file.Path;
import java.util.Optional;

public interface LoaderContext {

  public Path getPackagePath();

  public void addFunction(FlinkFnc flinkFnc);

  ErrorCollector getErrorCollector();

  Name registerTable(TableSource table, Optional<Name> alias);
}
