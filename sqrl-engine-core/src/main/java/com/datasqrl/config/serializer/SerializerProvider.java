package ai.datasqrl.config.serializer;

import com.esotericsoftware.kryo.Kryo;

import java.io.Serializable;

public interface SerializerProvider extends Serializable {

  Kryo getSerializer();

}
