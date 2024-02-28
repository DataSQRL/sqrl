package com.datasqrl.json;

import com.google.auto.service.AutoService;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

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
