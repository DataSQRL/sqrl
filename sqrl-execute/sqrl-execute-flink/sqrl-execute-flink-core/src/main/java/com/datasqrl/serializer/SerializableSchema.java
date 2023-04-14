package com.datasqrl.serializer;

import java.io.Serializable;
import java.util.List;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NoArgsConstructor;
import lombok.Singular;
import lombok.Value;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.table.types.DataType;

@Value
@NoArgsConstructor(force = true, access = AccessLevel.PRIVATE)
@AllArgsConstructor
@Builder
public class SerializableSchema implements Serializable {

  @Singular
  List<Pair<String, DataType>> columns;
  Boolean isWatermarkColumn;
  String watermarkName;
  String watermarkExpression;
  List<String> primaryKey;
}
