package com.datasqrl.io;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.io.tables.TableConfig;
import com.datasqrl.util.ServiceLoaderDiscovery;

public interface DynamicSinkFactory {

  TableConfig get(Name name);

}
