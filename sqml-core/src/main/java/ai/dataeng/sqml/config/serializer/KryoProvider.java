package ai.dataeng.sqml.config.serializer;

import ai.dataeng.sqml.ScriptDeployment;
import ai.dataeng.sqml.config.provider.SerializerProvider;
import ai.dataeng.sqml.config.scripts.ScriptBundle;
import ai.dataeng.sqml.config.scripts.SqrlQuery;
import ai.dataeng.sqml.config.scripts.SqrlScript;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import java.io.Serializable;

public class KryoProvider implements SerializerProvider, Serializable {
    @Override
    public Kryo getSerializer() {
        Kryo kryo = new Kryo();

        //These are only written to database
        kryo.register(ScriptDeployment.class,new JavaSerializer());
        kryo.register(ScriptBundle.class,new JavaSerializer());
        kryo.register(SqrlScript.class,new JavaSerializer());
        kryo.register(SqrlQuery.class,new JavaSerializer());
        return kryo;
    }
}
