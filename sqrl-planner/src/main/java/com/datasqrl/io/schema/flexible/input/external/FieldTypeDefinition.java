/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.io.schema.flexible.input.external;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public interface FieldTypeDefinition extends Serializable {

  String getType();

  List<FieldDefinition> getColumns();

  List<String> getTests();

  Map<String, Object> getCardinality();
}
