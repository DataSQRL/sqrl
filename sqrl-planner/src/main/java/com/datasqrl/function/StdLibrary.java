package com.datasqrl.function;

import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.module.SqrlModule;

public interface StdLibrary extends SqrlModule {
  NamePath getPath();
}
