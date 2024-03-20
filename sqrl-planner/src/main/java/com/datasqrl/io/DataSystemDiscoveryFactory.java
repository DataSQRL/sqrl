package com.datasqrl.io;

import com.datasqrl.config.SqrlConfig;
import com.datasqrl.io.tables.TableConfig;
import com.datasqrl.util.ServiceLoaderDiscovery;
import lombok.NonNull;

public interface DataSystemDiscoveryFactory extends DataSystemImplementationFactory {

  DataSystemDiscovery initialize(@NonNull TableConfig tableConfig);


}
