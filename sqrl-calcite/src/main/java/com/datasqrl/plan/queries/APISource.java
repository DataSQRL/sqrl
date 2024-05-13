package com.datasqrl.plan.queries;

import com.datasqrl.canonicalizer.Name;

public interface APISource {
  Name getName();
  String getSchemaDefinition();

  APISource clone(String schema);
}
