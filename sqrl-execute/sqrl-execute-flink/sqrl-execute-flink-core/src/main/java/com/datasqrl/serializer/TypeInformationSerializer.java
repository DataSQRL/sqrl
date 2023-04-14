package com.datasqrl.serializer;

import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.google.auto.service.AutoService;
import org.apache.flink.api.common.typeinfo.TypeInformation;

@AutoService(StdSerializer.class)
public class TypeInformationSerializer extends Base64Serializer<TypeInformation> {
  public TypeInformationSerializer() {
    super(TypeInformation.class);
  }
}
