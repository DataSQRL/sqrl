package ai.datasqrl.compile.loaders;

import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.plan.local.generate.Resolve;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class AbstractLoader {

    final ObjectMapper jsonMapper = new ObjectMapper();
    final YAMLMapper yamlMapper = new YAMLMapper();

    public static Path namepath2Path(Resolve.Env env, NamePath path) {
        Path filePath = env.getPackagePath();
        for (int i = 0; i < path.getNames().length; i++) {
            Name name = path.getNames()[i];
            filePath = filePath.resolve(name.getCanonical());
        }
        return filePath;
    }

    public static Set<Path> getAllFilesInPath(Path basePath, Pattern filePattern) {
        try (Stream<Path> files = Files.list(basePath)) {
            return files.filter(Files::isRegularFile)
                    .filter(f -> filePattern.matcher(f.getFileName().toString()).find())
                    .collect(Collectors.toSet());
        } catch (IOException e) {
            return Collections.EMPTY_SET;
        }
    }

    public <T> T mapJsonFile(Path path, Class<T> clazz) {
        return mapFile(jsonMapper, path, clazz);
    }

    public <T> T mapYAMLFile(Path path, Class<T> clazz) {
        return mapFile(yamlMapper, path, clazz);
    }

    public static <T> T mapFile(ObjectMapper mapper, Path path, Class<T> clazz) {
        try {
            return mapper.readValue(path.toFile(), clazz);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
