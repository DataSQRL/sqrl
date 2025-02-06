/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.io.schema.flexible.external;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

import com.datasqrl.schema.input.external.DatasetDefinition;
import com.datasqrl.util.StringNamedId;

public class SchemaDefinition implements Serializable {

  public String version;
  public List<DatasetDefinition> datasets;


  public static SchemaDefinition empty() {
    var def = new SchemaDefinition();
    def.datasets = Collections.EMPTY_LIST;
    def.version = StringNamedId.of("1").getId();
    return def;
  }

}
