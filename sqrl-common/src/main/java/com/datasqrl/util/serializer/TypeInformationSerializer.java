package com.datasqrl.util.serializer;

import org.apache.flink.api.common.typeinfo.TypeInformation;

public class TypeInformationSerializer extends Base64Serializer<TypeInformation> {
  public TypeInformationSerializer() {
    super(TypeInformation.class);
  }
}
