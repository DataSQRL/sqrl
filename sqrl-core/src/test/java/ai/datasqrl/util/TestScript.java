package ai.datasqrl.util;

import ai.datasqrl.util.data.Nutshop;
import ai.datasqrl.util.data.Retail;
import ai.datasqrl.util.junit.ArgumentProvider;
import lombok.Builder;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.Value;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

public interface TestScript {

    String getName();

    Path getRootPackageDirectory();

    Path getScript();

    @SneakyThrows
    default String getScriptContent() {
        return Files.readString(getScript());
    }

    List<String> getResultTables();

    List<Path> getGraphQLSchemas();

    default boolean dataSnapshot() {
        return true;
    }

    @Value
    @Builder
    static class Impl implements TestScript {

        @NonNull
        final String name;
        @NonNull
        final Path rootPackageDirectory;
        @NonNull
        final Path script;
        @NonNull
        final List<String> resultTables;
        @Builder.Default
        final boolean dataSnapshot = true;
        @Builder.Default @NonNull
        final List<Path> graphQLSchemas = List.of();

        @Override
        public String toString() {
            return name;
        }

        @Override
        public boolean dataSnapshot() {
            return dataSnapshot;
        }

    }

    static TestScript.Impl.ImplBuilder of(TestDataset dataset, Path script, String... resultTables) {
        return of(dataset.getRootPackageDirectory(),script, resultTables);
    }

    static TestScript.Impl.ImplBuilder of(Path rootPackage, Path script, String... resultTables) {
        String name = script.getFileName().toString();
        if (name.endsWith(".sqrl")) name = name.substring(0,name.length()-5);
        return Impl.builder().name(name).rootPackageDirectory(rootPackage).script(script)
                .resultTables(Arrays.asList(resultTables));
    }

        /*
    === STATIC METHODS ===
     */

    static List<TestScript> getAll() {
        ImmutableList.Builder b = new ImmutableList.Builder();
        b.addAll(Retail.INSTANCE.getTestScripts().values());
        b.addAll(Nutshop.INSTANCE.getScripts().subList(0,2));
        return b.build();
    }

    class AllScriptsProvider implements ArgumentsProvider {

        @Override
        public Stream<? extends Arguments> provideArguments(ExtensionContext extensionContext) throws Exception {
            return ArgumentProvider.of(getAll());
        }
    }

    class AllScriptsWithGraphQLSchemaProvider implements ArgumentsProvider {

        @Override
        public Stream<? extends Arguments> provideArguments(ExtensionContext extensionContext) throws Exception {
            return getAll().stream().flatMap(script ->
                    script.getGraphQLSchemas().stream().map(path -> Arguments.of(script,path)));
        }
    }


}
