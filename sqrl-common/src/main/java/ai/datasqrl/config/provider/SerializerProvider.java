package ai.datasqrl.config.provider;

import com.esotericsoftware.kryo.Kryo;

public interface SerializerProvider {

  Kryo getSerializer();

}
