package ai.datasqrl.compiler;

import ai.datasqrl.compile.loaders.DataSourceLoader;
import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.io.sources.dataset.TableSource;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.util.TestDataset;
import ai.datasqrl.util.data.Retail;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

public class LoaderTest {

    @Test
    public void testLoadingSources() {
        TestDataset example = Retail.INSTANCE;
        ErrorCollector errors = ErrorCollector.root();
        DataSourceLoader loader = new DataSourceLoader();
        Optional<TableSource> table = loader.readTable(example.getRootPackageDirectory(), NamePath.of("ecommerce-data","orders"), errors);
        assertFalse(errors.isFatal(), errors.toString());
        assertTrue(table.isPresent());
        assertEquals(table.get().getName(),Name.system("orders"));
    }

}
