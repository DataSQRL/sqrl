package ai.datasqrl.compile.loaders;

import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.plan.local.generate.Resolve;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.google.common.base.Preconditions;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class AbstractLoader implements Loader {

    final ObjectMapper jsonMapper = new ObjectMapper();
    final YAMLMapper yamlMapper = new YAMLMapper();

    public <T> T mapJsonFile(Path path, Class<T> clazz) {
        return mapFile(jsonMapper, path, clazz);
    }

    public <T> T mapYAMLFile(Path path, Class<T> clazz) {
        return mapFile(yamlMapper, path, clazz);
    }

    public static Path namepath2Path(Resolve.Env env, NamePath path) {
        return namepath2Path(env.getPackagePath(),path);
    }

    public static Path namepath2Path(Path basePath, NamePath path) {
        Path filePath = basePath;
        for (int i = 0; i < path.getNames().length; i++) {
            Name name = path.getNames()[i];
            filePath = filePath.resolve(name.getCanonical());
        }
        return filePath;
    }

    @Override
    public Collection<Name> loadAll(Resolve.Env env, NamePath basePath) {
        return getAllFilesInPath(namepath2Path(env,basePath)).stream()
                .map(p -> handles(p))
                .filter(Optional::isPresent)
                .map(name -> {
                    Name resovledName = Name.system(name.get());
                    Preconditions.checkArgument(resovledName.getCanonical().equals(name.get()));
                    boolean loaded =  load(env,basePath.concat(resovledName),Optional.empty());
                    Preconditions.checkArgument(loaded);
                    return resovledName;
                })
                .collect(Collectors.toSet());
    }

    public static Set<Path> getAllFilesInPath(Path directory) {
        try (Stream<Path> files = Files.list(directory)) {
            return files.filter(Files::isRegularFile)
                    .collect(Collectors.toSet());
        } catch (IOException e) {
            return Collections.EMPTY_SET;
        }
    }

    public static <T> T mapFile(ObjectMapper mapper, Path path, Class<T> clazz) {
        try {
            return mapper.readValue(path.toFile(), clazz);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
