package ai.datasqrl.io;

import ai.datasqrl.AbstractSQRLIntegrationTest;
import ai.datasqrl.IntegrationTestSettings;
import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.io.sources.dataset.SourceDataset;
import ai.datasqrl.io.sources.dataset.SourceTable;
import ai.datasqrl.io.sources.stats.SourceTableStatistics;
import ai.datasqrl.util.TestDataset;
import ai.datasqrl.util.data.BookClub;
import ai.datasqrl.util.data.C360;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestDataSetMonitoring extends AbstractSQRLIntegrationTest {

    @ParameterizedTest
    @ArgumentsSource(MonitoringArgumentsProvider.class)
    public void testDatasetMonitoring(TestDataset example, IntegrationTestSettings.StreamEngine engine) {
        initialize(IntegrationTestSettings.builder().monitorSources(true).engine(engine).build());

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

    static class MonitoringArgumentsProvider implements ArgumentsProvider {

        TestDataset[] datasets = {BookClub.INSTANCE, C360.INSTANCE};

        @Override
        public Stream<? extends Arguments> provideArguments(ExtensionContext extensionContext) throws Exception {
            List<Arguments> args = new ArrayList<>();
            for (TestDataset dataset : datasets) {
                for (IntegrationTestSettings.StreamEngine engine : IntegrationTestSettings.StreamEngine.values()) {
                    args.add(Arguments.of(dataset, engine));
                }
            }
            return args.stream();
        }
    }

}
