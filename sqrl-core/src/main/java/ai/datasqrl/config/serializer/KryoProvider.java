package ai.datasqrl.config.serializer;

import ai.datasqrl.config.provider.SerializerProvider;
import com.esotericsoftware.kryo.Kryo;

import java.io.Serializable;

public class KryoProvider implements SerializerProvider, Serializable {

  @Override
  public Kryo getSerializer() {
    Kryo kryo = new Kryo();

    //These are only written to database
    return kryo;
  }
}
