package ai.dataeng.sqml.config.metadata;

import lombok.NonNull;

import java.io.Closeable;
import java.io.Serializable;
import java.util.Set;

public interface MetadataStore extends Closeable {

    public void close();

    public<T> void put(@NonNull T value, @NonNull String firstKey, String... moreKeys);

    public<T> T get(@NonNull Class<T> clazz, @NonNull String firstKey, String... moreKeys);

    public Set<String> getSubKeys(String... keys);

}
