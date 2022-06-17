package ai.datasqrl.io;

import ai.datasqrl.api.ConfigurationTest;
import ai.datasqrl.io.formats.Format;
import ai.datasqrl.io.formats.JsonLineFormat;
import ai.datasqrl.io.formats.TextLineFormat;
import ai.datasqrl.io.sources.SourceRecord;
import com.google.common.base.Preconditions;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

public class FormatTest {

    @Test
    public void testJson() {
        List<SourceRecord.Raw> input = parseStream(fileStream(ConfigurationTest.BOOK_FILES),
                new JsonLineFormat.JsonLineParser()).collect(Collectors.toList());
        testInput(input, 4,2, 2);
    }

    public static void testInput(List<SourceRecord.Raw> input, int total, int minNumFields, int maxNumFields) {
        assertEquals(total,input.size());
        if (total > 0) {
            int min = input.stream().map(r -> r.getData().size()).min(Integer::compareTo).get();
            int max = input.stream().map(r -> r.getData().size()).max(Integer::compareTo).get();
            assertTrue(min >= minNumFields, "Min num fields: " + min);
            assertTrue(max <= minNumFields, "Max num fields: " + max);
        }
    }

    public static Stream<SourceRecord.Raw> parseStream(Stream<String> textSource,
                                                               TextLineFormat.Parser textparser) {
        return textSource.map(s -> {
                    Format.Parser.Result result = textparser.parse(s);
                    if (!result.isSuccess()) throw new RuntimeException(
                            String.format("Could not parse line [{}] due to error: {}", s, result.getErrorMsg()));
                    return result;
                })
                .map(r -> new SourceRecord.Raw(r.getRecord(), r.getSourceTime()));
    }

    @SneakyThrows
    public static Stream<String> fileStream(Path... paths) {
        Preconditions.checkArgument(paths!=null);
        return Arrays.stream(paths).flatMap(p -> {
            try {
                return Files.lines(p);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

}
