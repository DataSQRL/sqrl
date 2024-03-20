/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.metadata.serializer;

import com.esotericsoftware.kryo.Kryo;

public class KryoProvider implements SerializerProvider {

  @Override
  public Kryo getSerializer() {
    Kryo kryo = new Kryo();

    //These are only written to database
    return kryo;
  }
}
