package com.datasqrl.plan.table;

import com.datasqrl.canonicalizer.Name;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import org.apache.commons.lang3.StringUtils;

@AllArgsConstructor
public class TableIdFactory {
  AtomicInteger uniqueTableId;

  public Name createTableId(@NonNull Name name) {
    return createTableId(name, null);
  }

  public Name createTableId(@NonNull Name name, String type) {
    if (!StringUtils.isEmpty(type)) {
      name = name.suffix(type);
    }
    return name.suffix(Integer.toString(uniqueTableId.incrementAndGet()));
  }

  public Name createImportTableId(Name name) {
    return createTableId(name, "i");
  }

  public Name createQueryTableId(Name name) {
    return createTableId(name, "q");
  }
}
