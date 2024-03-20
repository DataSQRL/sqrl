/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.metadata.serializer;

import com.esotericsoftware.kryo.Kryo;

import java.io.Serializable;

public interface SerializerProvider extends Serializable {

  Kryo getSerializer();

}
