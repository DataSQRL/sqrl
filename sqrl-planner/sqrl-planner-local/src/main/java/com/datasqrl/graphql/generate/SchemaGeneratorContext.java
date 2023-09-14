/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.graphql.generate;

import com.datasqrl.schema.SQRLTable;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import java.util.List;
import lombok.Getter;
import org.apache.calcite.schema.FunctionParameter;

@Getter
public class SchemaGeneratorContext {

  private final BiMap<String, SQRLTable> names = HashBiMap.create();

}
