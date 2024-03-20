package com.datasqrl.io.impl.print;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.DataSystemDiscovery;
import com.datasqrl.io.ExternalDataType;
import com.datasqrl.io.tables.TableConfig;
import java.util.Optional;
import lombok.NonNull;

public class PrintDataSystemDiscovery extends DataSystemDiscovery.Base {

  public static final String PREFIX_KEY = "prefix";

  public PrintDataSystemDiscovery(TableConfig baseTable) {
    super(baseTable);
  }

  @Override
  public boolean requiresFormat(ExternalDataType type) {
    return true;
  }

  @Override
  public Optional<TableConfig> discoverSink(@NonNull Name sinkName,
      @NonNull ErrorCollector errors) {
    return Optional.of(copyGeneric(sinkName, ExternalDataType.sink).build());
  }

}
