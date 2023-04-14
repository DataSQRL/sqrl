package com.datasqrl.function;

import com.datasqrl.module.SqrlModule;
import com.datasqrl.canonicalizer.NamePath;

public interface StdLibrary extends SqrlModule {
  NamePath getPath();
}
