package ai.datasqrl.config.scripts;

import ai.datasqrl.config.constraints.OptionalMinString;
import ai.datasqrl.config.util.FileUtil;
import ai.datasqrl.config.error.ErrorCollector;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import org.h2.util.StringUtils;

@Builder
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class FileScriptConfiguration {

    public static final String INPUT_SCHEMA_FILE_SUFFIX = ".input.yml";
    public static final String SCRIPT_FILE_EXTENSION = "sqrl";
    public static final String QUERY_FOLDER_NAME = "queries";
    public static final String[] GRAPHQL_FILE_EXTENSIONS = new String[]{"graphql", "graphqls"};

    @OptionalMinString
    private String name;
    @NonNull @NotNull @Size(min=3)
    private String path;
    private String version;

    /**
     * Builds a {@link ScriptBundle.Config} from a path on the local filesystem.
     *
     * If the path points to a file, it is expected that this file is an SQRL script and must have the sqrl extension.
     * If an input schema yml with the same name exists in the directory of that file, it is considered the input schema
     * for that script. In addition, any queries located in a subfolder named "queries" are parsed as queries.
     *
     * If the path points to a file, all sqrl files in that directory are added to the bundle. The file with the same
     * name as the bundle is considered the main script. Input schema yml files that have the same name as a script are
     * associated with that script. In addition, any queries located in a subfolder named "queries" are parsed as queries.
     *
     * @param errors
     * @return
     */
    public @Nullable ScriptBundle.Config getBundle(ErrorCollector errors) {
        ErrorCollector tempErrors = errors;
        if (!Strings.isNullOrEmpty(name)) tempErrors = errors.resolve(name);
        Path bundlePath;
        try {
            bundlePath = Path.of(path);
            if (!Files.exists(bundlePath) || !Files.isReadable(bundlePath)) {
                errors.fatal("Path does not exist or cannot be read: %s", path);
                return null;
            }
        } catch (InvalidPathException e) {
            errors.fatal("Path is invalid: %s", path);
            return null;
        }


        String resolvedName;
        List<SqrlScript.Config> scripts;
        if (Files.isDirectory(bundlePath)) {
            resolvedName = StringUtils.isNullOrEmpty(name)?bundlePath.getFileName().toString():name;
            ErrorCollector subErrors = errors.resolve(resolvedName);
            scripts = FileUtil.executeFileRead(bundlePath,
                    path -> {
                        return Files.list(bundlePath).filter(this::supportedScriptFile)
                                .map(p -> getScript(p,resolvedName,subErrors)).collect(Collectors.toList());
                    }, subErrors);
        } else if (Files.isRegularFile(bundlePath)) {
            if (!FileUtil.getExtension(bundlePath).equalsIgnoreCase(SCRIPT_FILE_EXTENSION)) {
                tempErrors.fatal("Expected path to be an .SQRL file but got: %s", path);
                return null;
            }
            resolvedName = StringUtils.isNullOrEmpty(name)?FileUtil.removeExtension(bundlePath):name;
            scripts = ImmutableList.of(getScript(bundlePath,resolvedName,errors.resolve(resolvedName)));
        } else {
            tempErrors.fatal("Expected a file or directory for path: %s", path);
            return null;
        }

        ErrorCollector queryErrors = errors.resolve(resolvedName);
        List<SqrlQuery.Config> queries = new ArrayList<>();
        Path queryPath = bundlePath.resolve(QUERY_FOLDER_NAME);
        if (Files.isDirectory(queryPath)) {
            queries = FileUtil.executeFileRead(queryPath,
                    path -> {
                return Files.list(path).filter(this::supportedQueryFile)
                    .map(p -> getQuery(p, queryErrors)).collect(Collectors.toList());
                }, queryErrors);
        }


        if (scripts==null || scripts.stream().anyMatch(Objects::isNull)
                || queries==null || queries.stream().anyMatch(Objects::isNull)) {
            return null;
        }

        return ScriptBundle.Config.builder().name(resolvedName).version(version)
                .scripts(scripts).queries(queries).build();
    }

    private SqrlScript.Config getScript(Path scriptPath, String bundleName,
                                        ErrorCollector errors) {
        String name = FileUtil.removeExtension(scriptPath);

        String scriptContent = FileUtil.executeFileRead(scriptPath, Files::readString, errors);

        String schemaContent = null;
        Path directory = scriptPath.getParent();
        Path schemaFile = directory.resolve(name + INPUT_SCHEMA_FILE_SUFFIX);
        if (Files.exists(schemaFile) && Files.isRegularFile(schemaFile)) {
            schemaContent = FileUtil.executeFileRead(schemaFile, Files::readString, errors);
        }

        return SqrlScript.Config.builder().name(name)
                .main(name.equalsIgnoreCase(bundleName))
                .content(scriptContent)
                .inputSchema(schemaContent)
                .build();
    }

    private boolean supportedScriptFile(Path p) {
        return Files.isRegularFile(p) && FileUtil.getExtension(p).equalsIgnoreCase(SCRIPT_FILE_EXTENSION);
    }

    private SqrlQuery.Config getQuery(Path queryPath, ErrorCollector errors) {
        String name = FileUtil.removeExtension(queryPath);

        String queryContent = FileUtil.executeFileRead(queryPath, Files::readString, errors);

        return SqrlQuery.Config.builder().name(name)
                .qraphQL(queryContent)
                .build();
    }

    private boolean supportedQueryFile(Path p) {
        if (!Files.isRegularFile(p)) return false;
        String ext = FileUtil.getExtension(p);
        for (String gql : GRAPHQL_FILE_EXTENSIONS) {
            if (gql.equalsIgnoreCase(ext)) return true;
        }
        return false;
    }

}
