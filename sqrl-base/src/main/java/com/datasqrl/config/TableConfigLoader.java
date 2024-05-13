package com.datasqrl.config;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.error.ErrorCollector;
import java.net.URI;
import lombok.NonNull;

public interface TableConfigLoader {

  TableConfig load(@NonNull URI uri, @NonNull Name name,
      @NonNull ErrorCollector errors);
}
