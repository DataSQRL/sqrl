/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.schema.input.external;

import java.io.Serializable;
import java.util.List;

public interface FieldTypeDefinition extends Serializable {

  String getType();

  List<FieldDefinition> getColumns();

  List<String> getTests();


}
