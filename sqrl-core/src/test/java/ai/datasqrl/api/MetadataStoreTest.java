package ai.datasqrl.api;

import ai.datasqrl.AbstractSQRLIntegrationTest;
import ai.datasqrl.IntegrationTestSettings;
import ai.datasqrl.config.SqrlSettings;
import ai.datasqrl.config.metadata.MetadataStore;
import ai.datasqrl.config.provider.JDBCConnectionProvider;
import ai.datasqrl.config.serializer.KryoProvider;
import com.google.common.collect.ImmutableSet;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

public class MetadataStoreTest extends AbstractSQRLIntegrationTest {

    MetadataStore meta = null;

    @BeforeEach
    public void setup() {
        initialize(IntegrationTestSettings.getDefault());
        meta = getStore(sqrlSettings);
    }

    public MetadataStore getStore(SqrlSettings settings) {
        JDBCConnectionProvider jdbc = settings.getJdbcConfiguration().getDatabase(
                settings.getEnvironmentConfiguration().getMetastore().getDatabase());
        return settings.getMetadataStoreProvider().openStore(jdbc, new KryoProvider());
    }

    @AfterEach
    public void tearDown() {
        meta.close();
    }

    @Test
    public void testStore() {
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

        meta.remove("key2","sub1","leaf2");
        assertEquals(meta.getSubKeys("key2","sub1"), ImmutableSet.of("leaf1"));
        assertEquals(null, meta.get(Value.class,"key2","sub1","leaf2"));

        meta.close();
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
