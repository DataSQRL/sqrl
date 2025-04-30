/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.loaders;

import java.util.Optional;

import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.module.SqrlModule;

public interface ModuleLoader {
  Optional<SqrlModule> getModule(NamePath namePath);
}
