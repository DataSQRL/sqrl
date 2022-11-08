package ai.datasqrl.util.data;

import ai.datasqrl.util.TestDataset;
import ai.datasqrl.util.TestScript;
import lombok.AllArgsConstructor;

import java.nio.file.Path;
import java.util.List;
import java.util.Set;

@AllArgsConstructor
public class Nutshop implements TestDataset {

    public static final Path BASE_PATH = Path.of("..","sqml-examples","nutshop");

    public static final Nutshop INSTANCE = new Nutshop("small");

    public static final Nutshop MEDIUM = new Nutshop("medium");

    final String size;

    @Override
    public String getName() {
        return "nutshop-"+size;
    }

    @Override
    public Path getDataDirectory() {
        return BASE_PATH.resolve("data-"+size);
    }

    @Override
    public Set<String> getTables() {
        return Set.of("products","orders");
    }

    @Override
    public Path getRootPackageDirectory() {
        return BASE_PATH;
    }

    public List<TestScript> getScripts() {
        return List.of(
                TestScript.of(this,BASE_PATH.resolve("customer360").resolve("nutshopv1-"+size+".sqrl"),
                "orders", "items", "totals", "customers", "products", "spending_by_month"),
                TestScript.of(this,BASE_PATH.resolve("customer360").resolve("nutshopv2-"+size+".sqrl"),
                        "orders", "items", "totals", "customers", "products", "spending_by_month",
                        "past_purchases", "volume_by_day"));
    }

    @Override
    public String toString() {
        return getName();
    }


}
