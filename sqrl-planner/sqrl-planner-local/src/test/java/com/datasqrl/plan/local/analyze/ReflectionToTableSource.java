package com.datasqrl.plan.local.analyze;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.ExternalDataType;
import com.datasqrl.io.formats.JsonLineFormat;
import com.datasqrl.io.impl.CanonicalizerConfiguration;
import com.datasqrl.io.impl.inmem.InMemConnector;
import com.datasqrl.io.tables.TableConfig;
import com.datasqrl.io.tables.TableSource;
import com.datasqrl.io.util.TimeAnnotatedRecord;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.util.SqrlObjectMapper;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.util.ArrayList;
import java.util.List;
import lombok.SneakyThrows;

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
    ObjectMapper mapper = SqrlObjectMapper.INSTANCE;
    mapper.registerModule(new JavaTimeModule());
    mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    mapper.registerModule(new Jdk8Module());
    List<TimeAnnotatedRecord<String>> records = new ArrayList<>();
    for (Object obj : data) {
      String record = mapper.writeValueAsString(obj);
      records.add(new TimeAnnotatedRecord<>(record));
    }

    return records.toArray(TimeAnnotatedRecord[]::new);
  }
}
