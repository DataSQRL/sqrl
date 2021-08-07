package ai.dataeng.sqml.db.keyvalue;

import java.io.Serializable;

public interface HierarchyKeyValueStore {

    public void close();

    public<T> void put(T value, String firstKey, String... moreKeys);

    public<T> T get(Class<T> clazz, String firstKey, String... moreKeys);

    public interface Factory<T extends HierarchyKeyValueStore> extends Serializable {

        T open();

    }

}
