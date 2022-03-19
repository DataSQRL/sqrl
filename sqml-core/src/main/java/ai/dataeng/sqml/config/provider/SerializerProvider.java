package ai.dataeng.sqml.config.provider;

import com.esotericsoftware.kryo.Kryo;

public interface SerializerProvider {

    public Kryo getSerializer();

}
