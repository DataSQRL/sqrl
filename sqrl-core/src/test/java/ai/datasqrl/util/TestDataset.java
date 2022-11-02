package ai.datasqrl.util;

import ai.datasqrl.util.data.Retail;
import ai.datasqrl.util.junit.ArgumentProvider;
import com.google.common.base.Predicates;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public interface TestDataset {

    String getName();

    Path getDataDirectory();

    default Path getRootPackageDirectory() {
        return getDataDirectory();
    }

    ScriptBuilder getImports();

    default List<Path> getScripts() {
        return Collections.EMPTY_LIST;
    }

    /*
    === STATIC METHODS ===
     */

    static List<TestDataset> getAll() {
        return List.of(Retail.INSTANCE);
    }

    static Stream<? extends Arguments> generateAsArguments(List<? extends Object>... otherArgs) {
        return generateAsArguments(Predicates.alwaysTrue(),otherArgs);
    }

    static Stream<? extends Arguments> generateAsArguments(Predicate<TestDataset> filter, List<? extends Object>... otherArgs) {
        List<TestDataset> datasets = getAll().stream().filter(filter).collect(Collectors.toList());
        return generateAsArguments(datasets,otherArgs);
    }

    static Stream<? extends Arguments> generateAsArguments(List<TestDataset> datasets, List<? extends Object>... otherArgs) {
        List<List<? extends Object>> argumentLists = new ArrayList<>();
        argumentLists.add(datasets);
        if (otherArgs.length>0) argumentLists.addAll(Arrays.asList(otherArgs));
        return ArgumentProvider.crossProduct(argumentLists);
    }

    class AllProvider implements ArgumentsProvider {

        @Override
        public Stream<? extends Arguments> provideArguments(ExtensionContext extensionContext) throws Exception {
            return TestDataset.generateAsArguments();
        }
    }


}
