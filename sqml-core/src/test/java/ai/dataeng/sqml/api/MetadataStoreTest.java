package ai.dataeng.sqml.api;

import ai.dataeng.sqml.config.SqrlSettings;
import ai.dataeng.sqml.config.metadata.MetadataStore;
import ai.dataeng.sqml.config.provider.JDBCConnectionProvider;
import ai.dataeng.sqml.config.serializer.KryoProvider;
import com.google.common.collect.ImmutableSet;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

public class MetadataStoreTest {

    @BeforeEach
    public void deleteDatabase() throws IOException {
        FileUtils.cleanDirectory(ConfigurationTest.dbPath.toFile());
    }

    @Test
    public void testStore() {
        MetadataStore meta = getStore(ConfigurationTest.getDefaultSettings());
        Value[] values = new Value[10];
        for (int i = 0; i < values.length; i++) {
            values[i]=new Value(i, RandomStringUtils.random(10000,true,true));
        }
        assertTrue(meta.getSubKeys().isEmpty());
        meta.put(values[0],"key1");
        assertEquals(meta.get(Value.class,"key1"),values[0]);
        assertEquals(meta.getSubKeys(), ImmutableSet.of("key1"));

        meta.put(values[1],"key2","sub1","leaf1");
        meta.put(values[2],"key2","sub1","leaf2");
        assertEquals(meta.get(Value.class,"key2","sub1","leaf2"),values[2]);
        assertEquals(meta.getSubKeys(), ImmutableSet.of("key1","key2"));
        assertEquals(meta.getSubKeys("key2"), ImmutableSet.of("sub1"));
        assertEquals(meta.getSubKeys("key2","sub1"), ImmutableSet.of("leaf1","leaf2"));

        meta.close();
    }

    public static MetadataStore getStore(SqrlSettings settings) {
        JDBCConnectionProvider jdbc = settings.getJdbcConfiguration().getDatabase(
                settings.getEnvironmentConfiguration().getMetastore().getDatabase());
        return settings.getMetadataStoreProvider().openStore(jdbc, new KryoProvider());
    }

    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode
    @ToString
    public static class Value {

        private int number;
        private String str;

    }
}
