/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.io.tables;

import java.net.URI;
import java.util.Optional;

public interface TableSchema {

  String getSchemaType();

  String getDefinition();

  Optional<URI> getLocation();

}
