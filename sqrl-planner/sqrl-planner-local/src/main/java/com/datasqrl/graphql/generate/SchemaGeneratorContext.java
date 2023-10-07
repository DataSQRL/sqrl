/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.graphql.generate;

import com.datasqrl.graphql.inference.SqrlSchema2.SQRLTable;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import java.util.List;
import lombok.Getter;

@Getter
public class SchemaGeneratorContext {

  private final BiMap<String, SQRLTable> names = HashBiMap.create();

}
