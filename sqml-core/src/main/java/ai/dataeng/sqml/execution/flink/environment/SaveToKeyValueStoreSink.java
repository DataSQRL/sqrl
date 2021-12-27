package ai.dataeng.sqml.execution.flink.environment;

import ai.dataeng.sqml.catalog.persistence.keyvalue.HierarchyKeyValueStore;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class SaveToKeyValueStoreSink<T> extends RichSinkFunction<T> {

    private HierarchyKeyValueStore.Factory storeFactory;
    private Class<T> clazz;
    private String firstKey;
    private String[] moreKeys;

    private transient HierarchyKeyValueStore store;

    public SaveToKeyValueStoreSink(HierarchyKeyValueStore.Factory storeFactory, Class<T> clazz,
                                   String firstKey, String... moreKeys) {
        this.storeFactory = storeFactory;
        this.clazz = clazz;
        this.firstKey = firstKey;
        this.moreKeys = moreKeys;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        store = storeFactory.open();
    }

    @Override
    public void close() throws Exception {
        store.close();
        store = null;
    }

    @Override
    public void invoke(T value, Context context) throws Exception {
        store.put(value,firstKey,moreKeys);
    }

}
