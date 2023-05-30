/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.loaders;

import com.datasqrl.module.SqrlModule;
import com.datasqrl.canonicalizer.NamePath;
import java.util.Optional;

public interface ModuleLoader {
  Optional<SqrlModule> getModule(NamePath namePath);

  ModuleMetadata getModuleMetadata(NamePath path);
}
