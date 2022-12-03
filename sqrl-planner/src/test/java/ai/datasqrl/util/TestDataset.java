package com.datasqrl.util;

import com.datasqrl.util.data.Nutshop;
import com.datasqrl.util.data.Retail;
import com.datasqrl.util.junit.ArgumentProvider;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;

import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

public interface TestDataset {

    String getName();

    Path getDataDirectory();

    default String getFilePartPattern() {
        return null;
    }

    Set<String> getTables();

    default int getNumTables() {
        return getTables().size();
    }

    default Path getRootPackageDirectory() {
        return getDataDirectory();
    }

    /*
    === STATIC METHODS ===
     */

    static List<TestDataset> getAll() {
        return List.of(Retail.INSTANCE, Nutshop.INSTANCE);
    }

    class AllProvider implements ArgumentsProvider {

        @Override
        public Stream<? extends Arguments> provideArguments(ExtensionContext extensionContext) throws Exception {
            return ArgumentProvider.of(getAll());
        }
    }


}
