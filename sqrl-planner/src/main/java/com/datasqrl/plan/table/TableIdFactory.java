package com.datasqrl.plan.table;

import com.datasqrl.canonicalizer.Name;
import com.google.inject.Inject;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import org.apache.calcite.jdbc.SqrlSchema;
import org.apache.commons.lang3.StringUtils;

@AllArgsConstructor
public class TableIdFactory {
  private Map<Name, AtomicInteger> tableNameToIdMap;

  @Inject
  public TableIdFactory(SqrlSchema schema) {
    this(schema.getTableNameToIdMap());
  }

  public Name createTableId(@NonNull Name name) {
    return createTableId(name, null);
  }

  public Name createTableId(@NonNull Name name, String type) {
    if (!StringUtils.isEmpty(type)) {
      name = name.suffix(type);
    }
    AtomicInteger counter = tableNameToIdMap.computeIfAbsent(name, k -> new AtomicInteger(0));
    return name.suffix(Integer.toString(counter.incrementAndGet()));
  }
}
