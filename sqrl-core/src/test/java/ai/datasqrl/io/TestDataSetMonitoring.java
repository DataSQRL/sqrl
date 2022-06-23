package ai.datasqrl.io;

import ai.datasqrl.AbstractSQRLIntegrationTest;
import ai.datasqrl.IntegrationTestSettings;
import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.io.sources.dataset.SourceDataset;
import ai.datasqrl.io.sources.dataset.SourceTable;
import ai.datasqrl.io.sources.stats.SourceTableStatistics;
import ai.datasqrl.util.TestDataset;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestDataSetMonitoring extends AbstractSQRLIntegrationTest {

    @ParameterizedTest
    @ArgumentsSource(TestDatasetPlusStreamEngine.class)
    public void testDatasetMonitoring(TestDataset example, IntegrationTestSettings.EnginePair engine) {
        initialize(IntegrationTestSettings.builder().monitorSources(true).stream(engine.getStream()).database(engine.getDatabase()).build());

        ErrorCollector errors = ErrorCollector.root();
        sourceRegistry.addOrUpdateSource(example.getName(), example.getSource(), errors);
        assertFalse(errors.isFatal(), errors.toString());

        SourceDataset ds = sourceRegistry.getDataset(example.getName());
        Map<String,Integer> tblCounts = example.getTableCounts();
        assertEquals(tblCounts.size(),ds.getTables().size());
        for (Map.Entry<String,Integer> tbl : tblCounts.entrySet()) {
            SourceTable table = ds.getTable(tbl.getKey());
            SourceTableStatistics stats = table.getStatistics();
            assertNotNull(stats);
            assertEquals(tbl.getValue().longValue(),stats.getCount());
        }
    }

    static class TestDatasetPlusStreamEngine implements ArgumentsProvider {

        @Override
        public Stream<? extends Arguments> provideArguments(ExtensionContext extensionContext) throws Exception {
            return TestDataset.generateAsArguments(List.of(
                    new IntegrationTestSettings.EnginePair(IntegrationTestSettings.DatabaseEngine.INMEMORY, IntegrationTestSettings.StreamEngine.INMEMORY),
                    new IntegrationTestSettings.EnginePair(IntegrationTestSettings.DatabaseEngine.POSTGRES, IntegrationTestSettings.StreamEngine.FLINK)
            ));
        }
    }

}
