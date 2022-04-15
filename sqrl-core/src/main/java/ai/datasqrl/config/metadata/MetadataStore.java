package ai.datasqrl.config.metadata;

import java.io.Closeable;
import java.util.Set;

import ai.datasqrl.parse.tree.name.Name;
import lombok.NonNull;

public interface MetadataStore extends Closeable {

    public void close();

    public<T> void put(@NonNull T value, @NonNull String firstKey, String... moreKeys);

    public<T> T get(@NonNull Class<T> clazz, @NonNull String firstKey, String... moreKeys);

    public boolean remove(@NonNull String firstKey, String... moreKeys);

    public Set<String> getSubKeys(String... keys);

    default String name2Key(Name name) {
        return name.getCanonical();
    }

}
