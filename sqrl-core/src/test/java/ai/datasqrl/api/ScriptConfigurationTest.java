package ai.datasqrl.api;

import ai.datasqrl.config.scripts.FileScriptConfiguration;
import ai.datasqrl.config.scripts.ScriptBundle;
import ai.datasqrl.config.scripts.SqrlQuery;
import ai.datasqrl.config.scripts.SqrlScript;
import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.schema.type.schema.external.DatasetDefinition;
import ai.datasqrl.schema.type.schema.external.TableDefinition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.util.List;

public class ScriptConfigurationTest {

    public static final Path NUTSHOP_DIR = Path.of("../sqml-examples/nutshop/");
    public static final Path NUTSHOP_BASIC = NUTSHOP_DIR.resolve("customer360");
    public static final Path NUTSHOP_ADV = NUTSHOP_DIR.resolve("customer360-adv");
    public static final Path NUTSHOP_API = NUTSHOP_DIR.resolve("customer360-api");


    @Test
    public void testSimpleScriptFromFile() {
        FileScriptConfiguration fileConfig = FileScriptConfiguration.builder()
                .path(NUTSHOP_BASIC.toAbsolutePath().toString())
                .build();

        ErrorCollector errors = ErrorCollector.root();
        ScriptBundle.Config bundleConfig = fileConfig.getBundle(errors);
        assertNotNull(bundleConfig, errors.toString());
        ScriptBundle bundle = bundleConfig.initialize(errors);
        assertNotNull(bundle, errors.toString());

        Assertions.assertEquals("customer360",bundle.getName().getCanonical());
        assertEquals(1, bundle.getScripts().size());
        assertEquals(0, bundle.getQueries().size());
        SqrlScript main = bundle.getMainScript();
        Assertions.assertEquals("customer360",main.getName().getCanonical());
        assertTrue(main.getSchema().datasets.isEmpty());
        assertTrue(main.isMain());
        assertTrue(main.getContent().length()> 500);
    }

    @Test
    public void testScriptWithSchema() {
        ScriptBundle bundle = getBundleFromPath(NUTSHOP_ADV);
        Assertions.assertEquals("customer360-adv",bundle.getName().getCanonical());
        SqrlScript main = bundle.getMainScript();
        Assertions.assertEquals("customer360",main.getName().getCanonical());
        assertEquals(1, main.getSchema().datasets.size());
        List<DatasetDefinition> datasets = main.getSchema().datasets;
        TableDefinition table = datasets.get(0).tables.get(0);
        assertEquals("Product",table.name);
    }

    @Test
    public void testScriptWithQueries() {
        ScriptBundle bundle = getBundleFromPath(NUTSHOP_API);
        Assertions.assertEquals("customer360-api",bundle.getName().getCanonical());
        SqrlScript main = bundle.getMainScript();
        Assertions.assertEquals("customer360",main.getName().getCanonical());
        assertEquals(1, bundle.getQueries().size());
        SqrlQuery q = bundle.getQueries().values().iterator().next();
        Assertions.assertEquals("products",q.getName().getCanonical());
        assertTrue(q.getQraphQL().length()> 100);
    }


    public static ScriptBundle getBundleFromPath(Path path) {
        FileScriptConfiguration fileConfig = FileScriptConfiguration.builder()
                .path(path.toAbsolutePath().toString())
                .build();

        ErrorCollector errors = ErrorCollector.root();
        ScriptBundle.Config bundleConfig = fileConfig.getBundle(errors);
        assertNotNull(bundleConfig, errors.toString());
        ScriptBundle bundle = bundleConfig.initialize(errors);
        assertNotNull(bundle, errors.toString());
        return bundle;
    }

}
