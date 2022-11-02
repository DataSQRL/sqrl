package ai.datasqrl.util.data;

import ai.datasqrl.io.impl.file.DirectoryDataSystem;
import ai.datasqrl.util.ScriptBuilder;
import ai.datasqrl.util.ScriptComplexity;
import ai.datasqrl.util.TestDataset;
import ai.datasqrl.util.TestResources;
import com.google.common.collect.ImmutableMap;

import java.nio.file.Path;
import java.util.Map;

public class BookClub implements TestDataset {

    public static BookClub INSTANCE = new BookClub();

    public static final Path DATA_DIR = TestResources.RESOURCE_DIR.resolve("bookclub");
    public static final Path[] BOOK_FILES = new Path[]{DATA_DIR.resolve("book_001.json"), DATA_DIR.resolve("book_002.json")};

    @Override
    public String getName() {
        return "bookclub";
    }

    @Override
    public DirectoryDataSystem getSource() {
        return DirectoryDataSystem.builder()
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

    @Override
    public ScriptBuilder getImports() {
        ScriptBuilder builder = new ScriptBuilder();
        builder.append("IMPORT data.book");
        builder.append("IMPORT data.person");
        return builder;
    }

}
