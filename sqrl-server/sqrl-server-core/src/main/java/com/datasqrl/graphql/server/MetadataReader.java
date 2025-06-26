package com.datasqrl.graphql.server;

import graphql.schema.DataFetchingEnvironment;

public interface MetadataReader {

  Object read(DataFetchingEnvironment environment, String name);
}
