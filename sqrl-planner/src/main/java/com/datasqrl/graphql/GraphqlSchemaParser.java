package com.datasqrl.graphql;

import com.google.inject.Inject;

import graphql.schema.idl.SchemaParser;
import lombok.experimental.Delegate;

public class GraphqlSchemaParser extends SchemaParser {
  @Delegate
  SchemaParser parser;
  @Inject
  public GraphqlSchemaParser() {
    this.parser = new SchemaParser();
  }
}
