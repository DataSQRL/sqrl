package ai.datasqrl.compiler;

import ai.datasqrl.AbstractEngineIT;
import ai.datasqrl.IntegrationTestSettings;
import ai.datasqrl.metadata.MetadataStore;
import com.google.common.collect.ImmutableSet;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MetadataStoreTestIT extends AbstractEngineIT {

    MetadataStore meta = null;

    public void setup(IntegrationTestSettings.DatabaseEngine database) {
        initialize(IntegrationTestSettings.getEngines(IntegrationTestSettings.StreamEngine.INMEMORY, database));
        meta = engineSettings.getMetadataStoreProvider().openStore();
    }

    @AfterEach
    public void tearDown() {
        meta.close();
    }

    @ParameterizedTest
    @ArgumentsSource(DatabaseEngineProvider.class)
    public void testStore(IntegrationTestSettings.DatabaseEngine database) {
        setup(database);
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

    static class DatabaseEngineProvider implements ArgumentsProvider {

        @Override
        public Stream<? extends Arguments> provideArguments(ExtensionContext extensionContext) throws Exception {
            return Stream.of(IntegrationTestSettings.DatabaseEngine.INMEMORY, IntegrationTestSettings.DatabaseEngine.POSTGRES)
                    .map(Arguments::of);
        }
    }
}
