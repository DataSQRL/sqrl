package ai.datasqrl.util;

import ai.datasqrl.IntegrationTestSettings;
import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.config.scripts.ScriptBundle;
import ai.datasqrl.environment.Environment;
import ai.datasqrl.io.impl.file.DirectorySourceImplementation;
import ai.datasqrl.util.data.BookClub;
import ai.datasqrl.util.data.C360;
import ai.datasqrl.util.junit.ArgumentProvider;
import com.google.common.base.Predicates;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;

import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertFalse;

public interface TestDataset {

    String getName();

    DirectorySourceImplementation getSource();

    default void registerSource(Environment env) {
        ErrorCollector errors = ErrorCollector.root();
        env.getDatasetRegistry().addOrUpdateSource(getName(), getSource(), errors);
        assertFalse(errors.isFatal(),errors.toString());
    }

    Map<String, Integer> getTableCounts();

    String getScriptContent(ScriptComplexity complexity);

    Optional<String> getInputSchema();

    default TestScriptBundleBuilder buildBundle() {
        return new TestScriptBundleBuilder(this);
    }

    static List<TestDataset> getAll() {
        return List.of(BookClub.INSTANCE, C360.INSTANCE);
    }

    static Stream<? extends Arguments> generateAsArguments(List<? extends Object>... otherArgs) {
        return generateAsArguments(Predicates.alwaysTrue(),otherArgs);
    }

    static Stream<? extends Arguments> generateAsArguments(Predicate<TestDataset> filter, List<? extends Object>... otherArgs) {
        List<TestDataset> datasets = getAll().stream().filter(filter).collect(Collectors.toList());
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
