package ai.datasqrl.config.serializer;

import ai.datasqrl.config.provider.SerializerProvider;
import ai.datasqrl.config.scripts.ScriptBundle;
import ai.datasqrl.config.scripts.SqrlQuery;
import ai.datasqrl.config.scripts.SqrlScript;
import ai.datasqrl.server.ScriptDeployment;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import java.io.Serializable;

public class KryoProvider implements SerializerProvider, Serializable {

  @Override
  public Kryo getSerializer() {
    Kryo kryo = new Kryo();

    //These are only written to database
    kryo.register(ScriptDeployment.class, new JavaSerializer());
    kryo.register(ScriptBundle.class, new JavaSerializer());
    kryo.register(SqrlScript.class, new JavaSerializer());
    kryo.register(SqrlQuery.class, new JavaSerializer());
    return kryo;
  }
}
