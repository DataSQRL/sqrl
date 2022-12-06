package com.datasqrl.graphql.generate;

import com.datasqrl.schema.SQRLTable;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import lombok.Getter;

@Getter
public class SchemaGeneratorContext {

  private final BiMap<String, SQRLTable> names = HashBiMap.create();
}
