package com.datasqrl.loaders;

import java.util.Optional;

import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.module.SqrlModule;

public interface ObjectLoader {

  Optional<SqrlModule> load(NamePath namePath);
}
