package com.datasqrl.graphql;

import com.datasqrl.canonicalizer.Name;

public interface APISource {
  Name getName();
  String getSchemaDefinition();

  APISource clone(String schema);
}
