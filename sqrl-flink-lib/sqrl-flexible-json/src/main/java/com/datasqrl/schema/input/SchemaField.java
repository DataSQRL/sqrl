/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.schema.input;

import java.io.Serializable;

import com.datasqrl.canonicalizer.Name;

public interface SchemaField extends Serializable {

  Name getName();
}
