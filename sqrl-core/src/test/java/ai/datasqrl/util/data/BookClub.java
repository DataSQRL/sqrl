package ai.datasqrl.util.data;

import ai.datasqrl.config.scripts.ScriptBundle;
import ai.datasqrl.config.scripts.SqrlScript;
import ai.datasqrl.io.impl.file.DirectorySourceImplementation;
import ai.datasqrl.util.ScriptComplexity;
import ai.datasqrl.util.TestDataset;
import ai.datasqrl.util.TestResources;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class BookClub implements TestDataset {

    public static BookClub INSTANCE = new BookClub();

    public static final Path DATA_DIR = TestResources.RESOURCE_DIR.resolve("bookclub");
    public static final Path[] BOOK_FILES = new Path[]{DATA_DIR.resolve("book_001.json"), DATA_DIR.resolve("book_002.json")};

    @Override
    public String getName() {
        return "bookclub";
    }

    @Override
    public DirectorySourceImplementation getSource() {
        return DirectorySourceImplementation.builder()
                .uri(BookClub.DATA_DIR.toAbsolutePath().toString())
                .build();
    }

    @Override
    public Map<String, Integer> getTableCounts() {
        return ImmutableMap.of("person",5,"book",4);
    }

    @Override
    public String getScriptContent(ScriptComplexity complexity) {
        return "IMPORT data.book;";
    }

    public Optional<String> getInputSchema() {
        return Optional.empty();
    }
}
