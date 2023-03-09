package com.datasqrl.plan.local.analyze;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.ExternalDataType;
import com.datasqrl.io.formats.JsonLineFormat;
import com.datasqrl.io.impl.CanonicalizerConfiguration;
import com.datasqrl.io.impl.inmem.InMemConnector;
import com.datasqrl.io.tables.TableConfig;
import com.datasqrl.io.tables.TableSource;
import com.datasqrl.io.util.TimeAnnotatedRecord;
import com.datasqrl.name.NamePath;
import java.util.ArrayList;
import java.util.List;
import lombok.SneakyThrows;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.ObjectMapper;

public class ReflectionToTableSource {

  public static TableSource createTableSource(Class clazz, List<?> data) {
    TableConfig tableConfig = new TableConfig(ExternalDataType.source,
        CanonicalizerConfiguration.system,
        "UTF-8", new JsonLineFormat.Configuration(),
        clazz.getSimpleName().toLowerCase(), clazz.getSimpleName().toLowerCase(), "fixed",
        new InMemConnector("in-mem-" + clazz.getSimpleName().toLowerCase(), convertToData(data)));

    return tableConfig.initializeSource(ErrorCollector.root(),
        NamePath.of("ecommerce-data"),
        UtbToFlexibleSchema.createFlexibleSchema(ReflectionToUt.reflection(clazz)));
  }

  @SneakyThrows
  private static TimeAnnotatedRecord[] convertToData(List<?> data) {
    ObjectMapper mapper = new ObjectMapper();
    List<TimeAnnotatedRecord<String>> records = new ArrayList<>();
    for (Object obj : data) {
      records.add(new TimeAnnotatedRecord<>(mapper.writeValueAsString(obj)));
    }

    return records.toArray(TimeAnnotatedRecord[]::new);
  }
}
