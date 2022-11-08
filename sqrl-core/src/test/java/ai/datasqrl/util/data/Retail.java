package ai.datasqrl.util.data;

import ai.datasqrl.util.ScriptBuilder;
import ai.datasqrl.util.TestDataset;
import ai.datasqrl.util.TestScript;

import java.nio.file.Path;
import java.util.List;
import java.util.Set;

public class Retail implements TestDataset {

    public static final Path BASE_PATH = Path.of("..","sqml-examples","retail");

    public static final Retail INSTANCE = new Retail();

    @Override
    public String getName() {
        return "ecommerce-data";
    }

    @Override
    public Path getDataDirectory() {
        return BASE_PATH.resolve("data");
    }

    @Override
    public Set<String> getTables() {
        return Set.of("customer","product","orders");
    }

    @Override
    public Path getRootPackageDirectory() {
        return BASE_PATH;
    }

    public List<TestScript> getScripts() {
        return List.of(TestScript.of(this,BASE_PATH.resolve("c360-orderstats.sqrl"),
                "orders", "entries", "totals", "customerorderstats"),
                TestScript.of(this,BASE_PATH.resolve("c360-full.sqrl"),
                        "orders", "entries", "customer", "category", "product", "total", "order_again",
                        "_spending_by_month_category", "favorite_categories",
                        "order_stats", "newcustomerpromotion"));
    }

    public ScriptBuilder getImports() {
        ScriptBuilder builder = new ScriptBuilder();
        builder.append("IMPORT ecommerce-data.Customer");
        builder.append("IMPORT ecommerce-data.Orders");
        builder.append("IMPORT ecommerce-data.Product");
        return builder;
    }

    @Override
    public String toString() {
        return getName();
    }


}
