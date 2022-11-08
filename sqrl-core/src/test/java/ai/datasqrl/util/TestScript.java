package ai.datasqrl.util;

import ai.datasqrl.util.data.Retail;
import ai.datasqrl.util.junit.ArgumentProvider;
import lombok.Value;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

public interface TestScript {

    String getName();

    Path getRootPackageDirectory();

    Path getScript();

    List<String> getResultTables();

    @Value
    static class Impl implements TestScript {

        final String name;
        final Path rootPackageDirectory;
        final Path script;
        final List<String> resultTables;

        @Override
        public String toString() {
            return name;
        }

    }

    static TestScript of(TestDataset dataset, Path script, String... resultTables) {
        return of(dataset.getRootPackageDirectory(),script, resultTables);
    }

    static TestScript of(Path rootPackage, Path script, String... resultTables) {
        String name = script.getFileName().toString();
        if (name.endsWith(".sqrl")) name = name.substring(0,name.length()-5);
        return new Impl(name, rootPackage, script, Arrays.asList(resultTables));
    }

        /*
    === STATIC METHODS ===
     */

    static List<TestScript> getAll() {
        ImmutableList.Builder b = new ImmutableList.Builder();
        b.addAll(Retail.INSTANCE.getScripts().subList(0,2));
        return b.build();
    }

    class AllProvider implements ArgumentsProvider {

        @Override
        public Stream<? extends Arguments> provideArguments(ExtensionContext extensionContext) throws Exception {
            return ArgumentProvider.of(getAll());
        }
    }


}
