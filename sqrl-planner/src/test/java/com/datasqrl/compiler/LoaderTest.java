package com.datasqrl.compiler;

import com.datasqrl.compile.loaders.DataSource;
import com.datasqrl.config.error.ErrorCollector;
import com.datasqrl.io.sources.dataset.TableSource;
import com.datasqrl.parse.tree.name.Name;
import com.datasqrl.parse.tree.name.NamePath;
import com.datasqrl.util.TestDataset;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

public class LoaderTest {

    @ParameterizedTest
    @ArgumentsSource(TestDataset.AllProvider.class)
    public void testLoadingSources(TestDataset example) {
        ErrorCollector errors = ErrorCollector.root();
        DataSource.Loader loader = new DataSource.Loader();
        for (String tblName : example.getTables()) {
            Optional<TableSource> table = loader.readTable(example.getRootPackageDirectory(), NamePath.of(example.getName(),tblName), errors);
            assertFalse(errors.isFatal(), errors.toString());
            assertTrue(table.isPresent());
            assertEquals(table.get().getName(),Name.system(tblName));
        }
    }

}
