/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.schema.input.external;

import java.io.Serializable;
import java.util.List;

public class DatasetDefinition implements Serializable {

  public String name;
  public String version;
  public String description;
  public List<TableDefinition> tables;
}
