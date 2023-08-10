package com.datasqrl.calcite.convert;

import lombok.Value;
import org.apache.calcite.sql.SqlNode;

import java.util.List;
import java.util.Map;

@Value
public class SchemaMetadata {
  Map<List<String>, SqlNode> nodeMapping;
  Map<List<String>, List<String>> joinMapping;
  Map<List<String>, List<String>> tableArgPositions;
}
