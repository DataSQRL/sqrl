package com.datasqrl.io;

import com.datasqrl.config.DataStreamSourceFactory;
import com.datasqrl.config.FlinkSourceFactoryContext;
import com.datasqrl.io.mem.MemoryConnectorFactory;
import com.datasqrl.io.util.TimeAnnotatedRecord;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.common.base.Preconditions;
import lombok.SneakyThrows;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

public abstract class InMemSourceFactory implements DataStreamSourceFactory {

  private final String name;
  private final Map<String,List<?>> dataTables;

  public InMemSourceFactory(String name, Map<String,List<?>> data) {
    this.name = MemoryConnectorFactory.SYSTEM_NAME_PREFIX + name;
    this.dataTables = data;
  }

  @Override
  public String getSourceName() {
    return name;
  }

  @Override
  public SingleOutputStreamOperator<TimeAnnotatedRecord<String>> create(
      FlinkSourceFactoryContext context) {
    FlinkSourceFactoryContext ctx = (FlinkSourceFactoryContext) context;
    String tblName = ctx.getTableConfig().getBase().getIdentifier();
    Preconditions.checkArgument(dataTables.containsKey(tblName), "Could not find table: %s", tblName);
    List<?> data = dataTables.get(tblName);

    return ctx.getEnv()
        .fromElements(convertToData(data));
  }

  @SneakyThrows
  private static TimeAnnotatedRecord[] convertToData(List<?> data) {
    ObjectMapper mapper = new ObjectMapper()
        .registerModule(new JavaTimeModule())
        .registerModule(new Jdk8Module())
        .setSerializationInclusion(JsonInclude.Include.NON_NULL);
    mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    List<TimeAnnotatedRecord<String>> records = new ArrayList<>();
    for (Object obj : data) {
      String record = mapper.writeValueAsString(obj);
      records.add(new TimeAnnotatedRecord<>(record));
    }

    return records.toArray(TimeAnnotatedRecord[]::new);
  }
}
