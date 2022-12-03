package com.datasqrl.config.serializer;

import com.esotericsoftware.kryo.Kryo;

public class KryoProvider implements SerializerProvider {

  @Override
  public Kryo getSerializer() {
    Kryo kryo = new Kryo();

    //These are only written to database
    return kryo;
  }
}
