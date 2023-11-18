package com.datasqrl.json;

import com.google.auto.service.AutoService;

@AutoService(GraphqlGeneratorMapping.class)
public class JsonGraphqlGeneratorMapping implements GraphqlGeneratorMapping {

  @Override
  public Class getConversionClass() {
    return FlinkJsonType.class;
  }

  @Override
  public String getScalarName() {
    return "JSON";
  }
}
