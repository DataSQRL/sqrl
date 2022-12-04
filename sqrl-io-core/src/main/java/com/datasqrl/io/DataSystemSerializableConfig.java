/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.io;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;

@JsonIgnoreProperties(value = "systemType", allowGetters = true)
public interface DataSystemSerializableConfig extends Serializable {

  String TYPE_KEY = "systemType";

  String getSystemType();

}
