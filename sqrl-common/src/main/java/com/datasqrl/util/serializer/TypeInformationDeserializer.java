package com.datasqrl.util.serializer;

import org.apache.flink.api.common.typeinfo.TypeInformation;

public class TypeInformationDeserializer extends Base64Deserializer<TypeInformation> {
  public TypeInformationDeserializer() {
    super(TypeInformation.class);
  }
}