package com.datasqrl.format;

import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonFormatOptions.MapNullKeyMode;
import org.apache.flink.formats.json.RowDataToJsonConverters;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.table.data.binary.BinaryRawValueData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RawType;

public class SqrlRowDataToJsonConverters extends RowDataToJsonConverters {

  public SqrlRowDataToJsonConverters(TimestampFormat timestampFormat,
      MapNullKeyMode mapNullKeyMode,
      String mapNullKeyLiteral) {
    super(timestampFormat, mapNullKeyMode, mapNullKeyLiteral);
  }

  @Override
  public RowDataToJsonConverter createConverter(LogicalType type) {

    switch (type.getTypeRoot()) {
      case RAW:
        //sqrl add raw type
        RawType rawType = (RawType) type;
        if (rawType.getOriginatingClass() == JsonNode.class) {
          return createJsonConverter((RawType) type);
        }
    }
    return super.createConverter(type);
  }


  private RowDataToJsonConverter createJsonConverter(RawType type) {
    return (mapper, reuse, value) -> {
      if (value == null) {
        return null;
      }
      BinaryRawValueData binaryRawValueData = (BinaryRawValueData) value;
      return (JsonNode) binaryRawValueData.toObject(type.getTypeSerializer());
    };
  }
}
