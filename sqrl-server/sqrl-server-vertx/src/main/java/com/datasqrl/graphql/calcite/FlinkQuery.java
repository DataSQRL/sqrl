package com.datasqrl.graphql.calcite;

import java.util.List;
import lombok.Value;
import org.apache.calcite.rel.type.RelDataType;

@Value
public class FlinkQuery {
  String name;
  List<String> catalogQueries;
  String query;
  RelDataType relDataType;
}
