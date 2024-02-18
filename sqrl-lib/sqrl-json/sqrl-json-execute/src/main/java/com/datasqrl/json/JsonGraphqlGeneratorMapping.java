package com.datasqrl.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.auto.service.AutoService;

@AutoService(GraphqlGeneratorMapping.class)
public class JsonGraphqlGeneratorMapping implements GraphqlGeneratorMapping {

  @Override
  public Class getConversionClass() {
    return JsonNode.class;
  }

  @Override
  public String getScalarName() {
    return "JSON";
  }
}
