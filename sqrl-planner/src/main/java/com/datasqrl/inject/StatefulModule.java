package com.datasqrl.inject;

import org.apache.calcite.jdbc.SqrlSchema;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;

/**
 * Contains stateful dependencies for planning of a single script
 */
public class StatefulModule extends AbstractModule {
  private final SqrlSchema schema;

  //todo add error collector
  public StatefulModule(SqrlSchema schema) {
    this.schema = schema;
  }
  @Override
  protected void configure() {
  }

  @Provides
  public SqrlSchema provideSqrlSchema() {
    return schema;
  }

}
