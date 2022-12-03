package com.datasqrl.io;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;

@JsonIgnoreProperties(value = "systemType", allowGetters = true)
public interface DataSystemSerializableConfig extends Serializable {

    String TYPE_KEY = "systemType";

    String getSystemType();

}
