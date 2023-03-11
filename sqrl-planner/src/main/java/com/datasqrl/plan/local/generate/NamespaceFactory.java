package com.datasqrl.plan.local.generate;

import com.google.inject.Inject;
import org.apache.calcite.jdbc.SqrlSchema;

public class NamespaceFactory {
  SqrlSchema schema;

  @Inject
  public NamespaceFactory(SqrlSchema schema) {
    this.schema = schema;
  }

  public Namespace createNamespace() {
    return new Namespace(schema);
  }
}
