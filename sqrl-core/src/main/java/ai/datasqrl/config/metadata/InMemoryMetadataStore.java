package ai.datasqrl.config.metadata;

import ai.datasqrl.config.engines.InMemoryDatabaseConfiguration;
import ai.datasqrl.config.provider.DatabaseConnectionProvider;
import ai.datasqrl.config.provider.MetadataStoreProvider;
import ai.datasqrl.config.provider.SerializerProvider;
import com.google.common.base.Preconditions;
import lombok.NonNull;
import lombok.Value;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class InMemoryMetadataStore implements MetadataStore {

    private static ThreadLocal<Map<Key,Object>> STORE = new ThreadLocal<>();

    public static void clearLocal() {
        Map<Key,Object> store = STORE.get();
        if (store!=null) store.clear();
    }

    @Override
    public void close() {
        //Nothing to do
    }

    private Map<Key,Object> store() {
        Map<Key,Object> store = STORE.get();
        if (store==null) {
            store = new HashMap<>();
            STORE.set(store);
        }
        return store;
    }

    @Override
    public <T> void put(@NonNull T value, @NonNull String firstKey, String... moreKeys) {
        store().put(new Key(firstKey,moreKeys),value);
    }

    @Override
    public <T> T get(@NonNull Class<T> clazz, @NonNull String firstKey, String... moreKeys) {
        return (T)store().get(new Key(firstKey,moreKeys));
    }

    @Override
    public boolean remove(@NonNull String firstKey, String... moreKeys) {
        return store().remove(new Key(firstKey,moreKeys))!=null;
    }

    @Override
    public Set<String> getSubKeys(String... keys) {
        final Key prefix = new Key(keys);
        return store().keySet().stream()
                .filter(k -> k.hasPrefix(prefix))
                .map(k -> k.keys.get(prefix.keys.size()))
                .collect(Collectors.toSet());
    }

    @Value
    private static class Key {

        List<String> keys;

        private Key(String firstKey, String... moreKeys) {
            String[] keys = new String[moreKeys.length+1];
            System.arraycopy(moreKeys,0,keys,1,moreKeys.length);
            keys[0] = firstKey;
            this.keys = List.of(keys);
        }

        private Key(String... keys) {
            this.keys = List.of(keys);
        }

        public boolean hasPrefix(Key key) {
            if (key.keys.size()>=this.keys.size()) return false;
            for (int i = 0; i < key.keys.size(); i++) {
                if (!key.keys.get(i).equals(this.keys.get(i))) return false;
            }
            return true;
        }

    }

    public static class Provider implements MetadataStoreProvider {

        @Override
        public MetadataStore openStore(DatabaseConnectionProvider dbConnection, SerializerProvider serializer) {
            Preconditions.checkArgument(dbConnection instanceof InMemoryDatabaseConfiguration.ConnectionProvider);
            return new InMemoryMetadataStore();
        }
    }

}
