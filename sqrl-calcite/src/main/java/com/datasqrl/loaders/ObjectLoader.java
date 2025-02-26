package com.datasqrl.loaders;

import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.module.SqrlModule;
import java.util.Optional;

public interface ObjectLoader {

  Optional<SqrlModule> load(NamePath namePath);
}
