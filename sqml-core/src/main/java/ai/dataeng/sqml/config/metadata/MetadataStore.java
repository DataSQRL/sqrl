package ai.dataeng.sqml.config.metadata;

import java.io.Closeable;
import java.util.Set;
import lombok.NonNull;

public interface MetadataStore extends Closeable {

    public void close();

    public<T> void put(@NonNull T value, @NonNull String firstKey, String... moreKeys);

    public<T> T get(@NonNull Class<T> clazz, @NonNull String firstKey, String... moreKeys);

    public Set<String> getSubKeys(String... keys);

}
