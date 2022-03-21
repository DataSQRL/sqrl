package ai.dataeng.sqml.config.scripts;

import ai.dataeng.sqml.config.ConfigurationError;
import ai.dataeng.sqml.config.constraints.OptionalMinString;
import ai.dataeng.sqml.config.util.FileUtil;
import ai.dataeng.sqml.type.basic.ProcessMessage;
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
    public @Nullable ScriptBundle.Config getBundle(ProcessMessage.ProcessBundle<ConfigurationError> errors) {
        String tempName = name!=null?name:"";
        Path bundlePath;
        try {
            bundlePath = Path.of(path);
            if (!Files.exists(bundlePath) || !Files.isReadable(bundlePath)) {
                errors.add(ConfigurationError.fatal(ConfigurationError.LocationType.SCRIPT,tempName,
                        "Path does not exist or cannot be read: %s", path));
                return null;
            }
        } catch (InvalidPathException e) {
            errors.add(ConfigurationError.fatal(ConfigurationError.LocationType.SCRIPT,tempName,"Path is invalid: %s", path));
            return null;
        }


        String resolvedName;
        List<SqrlScript.Config> scripts;
        if (Files.isDirectory(bundlePath)) {
            resolvedName = StringUtils.isNullOrEmpty(name)?bundlePath.getFileName().toString():name;
            scripts = FileUtil.executeFileRead(bundlePath,
                    path -> {
                        return Files.list(bundlePath).filter(this::supportedScriptFile)
                                .map(p -> getScript(p,resolvedName,errors)).collect(Collectors.toList());
                    }, ConfigurationError.LocationType.SCRIPT,name,errors);
        } else if (Files.isRegularFile(bundlePath)) {
            if (!FileUtil.getExtension(bundlePath).equalsIgnoreCase(SCRIPT_FILE_EXTENSION)) {
                errors.add(ConfigurationError.fatal(ConfigurationError.LocationType.SCRIPT,tempName,"Expected path to be an .SQRL file but got: %s", path));
                return null;
            }
            resolvedName = StringUtils.isNullOrEmpty(name)?FileUtil.removeExtension(bundlePath):name;
            scripts = ImmutableList.of(getScript(bundlePath,resolvedName,errors));
        } else {
            errors.add(ConfigurationError.fatal(ConfigurationError.LocationType.SCRIPT,tempName,"Expected a file or directory for path: %s", path));
            return null;
        }

        List<SqrlQuery.Config> queries = new ArrayList<>();
        Path queryPath = bundlePath.resolve(QUERY_FOLDER_NAME);
        if (Files.isDirectory(queryPath)) {
            queries = FileUtil.executeFileRead(queryPath,
                    path -> {
                return Files.list(path).filter(this::supportedQueryFile)
                    .map(p -> getQuery(p, resolvedName, errors)).collect(Collectors.toList());
                },
                    ConfigurationError.LocationType.SCRIPT,name,errors);
        }


        if (scripts==null || scripts.stream().anyMatch(Objects::isNull)
                || queries==null || queries.stream().anyMatch(Objects::isNull)) {
            return null;
        }

        return ScriptBundle.Config.builder().name(resolvedName).version(version)
                .scripts(scripts).queries(queries).build();
    }

    private SqrlScript.Config getScript(Path scriptPath, String bundleName,
                                        ProcessMessage.ProcessBundle<ConfigurationError> errors) {
        String name = FileUtil.removeExtension(scriptPath);

        String scriptContent = FileUtil.executeFileRead(scriptPath, Files::readString,
                                ConfigurationError.LocationType.SCRIPT,bundleName,errors);

        String schemaContent = null;
        Path directory = scriptPath.getParent();
        Path schemaFile = directory.resolve(name + INPUT_SCHEMA_FILE_SUFFIX);
        if (Files.exists(schemaFile) && Files.isRegularFile(schemaFile)) {
            schemaContent = FileUtil.executeFileRead(schemaFile, Files::readString,
                    ConfigurationError.LocationType.SCRIPT,bundleName,errors);
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

    private SqrlQuery.Config getQuery(Path queryPath, String bundleName, ProcessMessage.ProcessBundle<ConfigurationError> errors) {
        String name = FileUtil.removeExtension(queryPath);

        String queryContent = FileUtil.executeFileRead(queryPath, Files::readString,
                ConfigurationError.LocationType.SCRIPT,bundleName,errors);

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
