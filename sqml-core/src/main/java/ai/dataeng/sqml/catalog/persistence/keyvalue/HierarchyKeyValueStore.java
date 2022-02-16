package ai.dataeng.sqml.catalog.persistence.keyvalue;

import java.io.Serializable;
import java.util.Set;

public interface HierarchyKeyValueStore {

    public void close();

    public<T> void put(T value, String firstKey, String... moreKeys);

    public<T> T get(Class<T> clazz, String firstKey, String... moreKeys);

    public Set<String> getSubKeys(String... keys);

    public interface Factory<T extends HierarchyKeyValueStore> extends Serializable {

        T open();

    }

}
