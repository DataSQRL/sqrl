package com.datasqrl.io;

import com.datasqrl.config.SourceFactory;
import com.datasqrl.config.SourceFactoryContext;
import com.datasqrl.FlinkSourceFactoryContext;
import com.datasqrl.io.impl.inmem.InMemConnector;
import com.datasqrl.io.util.TimeAnnotatedRecord;
import com.google.auto.service.AutoService;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

@AutoService(SourceFactory.class)
public class InMemSourceFactory implements
    SourceFactory<SingleOutputStreamOperator<TimeAnnotatedRecord<String>>> {
  public static final String SYSTEM_TYPE = "inmem";

  private final String name;

  public InMemSourceFactory() {
    this(SYSTEM_TYPE);
  }

  public InMemSourceFactory(String name) {
    this.name = name;
  }

  @Override
  public String getEngine() {
    return "flink";
  }

  @Override
  public String getSourceName() {
    return name;
  }

  @Override
  public SingleOutputStreamOperator<TimeAnnotatedRecord<String>> create(
      DataSystemConnector connector, SourceFactoryContext context) {
    InMemConnector inMemConnector = (InMemConnector) connector;
    FlinkSourceFactoryContext ctx = (FlinkSourceFactoryContext) context;

    return ctx.getEnv()
        .fromElements(inMemConnector.getData());
  }
}
