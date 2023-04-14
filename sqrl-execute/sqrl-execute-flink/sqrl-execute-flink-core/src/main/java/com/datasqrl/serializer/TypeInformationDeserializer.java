package com.datasqrl.serializer;

import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.google.auto.service.AutoService;
import org.apache.flink.api.common.typeinfo.TypeInformation;

@AutoService(StdDeserializer.class)
public class TypeInformationDeserializer extends Base64Deserializer<TypeInformation> {
  public TypeInformationDeserializer() {
    super(TypeInformation.class);
  }
}