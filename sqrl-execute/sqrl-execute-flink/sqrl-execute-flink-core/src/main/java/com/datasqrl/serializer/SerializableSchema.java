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
  List<String> primaryKey;
  WaterMarkType waterMarkType;
  String watermarkName;
  String watermarkExpression;

  public enum WaterMarkType {

    NONE, //Table has no watermark
    COLUMN_BY_NAME, //Table has watermark that's based on the column 'watermarkName'
    SOURCE_WATERMARK; //Table has watermark that is inherited from source data stream

  }
}
