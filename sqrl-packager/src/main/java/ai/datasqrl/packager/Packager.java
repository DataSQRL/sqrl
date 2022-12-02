package ai.datasqrl.packager;

import ai.datasqrl.compile.loaders.DynamicExporter;
import ai.datasqrl.compile.loaders.DynamicLoader;
import ai.datasqrl.packager.config.Dependency;
import ai.datasqrl.packager.config.GlobalPackageConfiguration;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import lombok.NonNull;
import lombok.Value;
import org.apache.flink.calcite.shaded.org.apache.commons.io.FileUtils;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

@Value
public class Packager {

    public static final String BUILD_DIR_NAME = "build";
    public static final String SCHEMA_FILE_NAME = "schema.graphqls";
    public static final String PACKAGE_FILE_NAME = "package.json";
    public static final Set<String> EXCLUDED_DIRS = Set.of(BUILD_DIR_NAME, "deploy");

    ObjectMapper mapper;
    Path rootDir;
    Path scriptFile;
    Optional<Path> graphQLSchemaFile;
    JsonNode packageConfig;
    GlobalPackageConfiguration config;

    public Packager(@NonNull Path scriptFile, @NonNull Optional<Path> graphQLSchemaFile, @NonNull Path... packageFiles) {
        Preconditions.checkArgument(Files.isRegularFile(scriptFile));
        Preconditions.checkArgument(packageFiles!=null && packageFiles.length>0);
        this.mapper = new ObjectMapper();
        mapper.enable(SerializationFeature.INDENT_OUTPUT);
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        this.rootDir = scriptFile.getParent();
        this.scriptFile = scriptFile;
        this.graphQLSchemaFile = graphQLSchemaFile;
        try {
            JsonNode basePackage = mapper.readValue(packageFiles[0].toFile(), JsonNode.class);
            for (int i = 1; i < packageFiles.length; i++) {
                basePackage = mapper.readerForUpdating(basePackage).readValue(packageFiles[i].toFile());
            }
            this.packageConfig = basePackage;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        config = mapper.convertValue(packageConfig, GlobalPackageConfiguration.class);
    }

    public void inferDependencies() {
        LinkedHashMap<String, Dependency> dependencies = config.getDependencies();
        //TODO: add dependencies from imports; if the namepath prefix doesn't map onto directory or sqrl file it's an external dependency
        JsonNode mappedDepends = mapper.valueToTree(dependencies);
        ((ObjectNode)packageConfig).set(GlobalPackageConfiguration.DEPENDENCIES_NAME, mappedDepends);
    }

    public void populateBuildDir() {
        try {
            Path buildDir = rootDir.resolve(BUILD_DIR_NAME);
            try {
                Files.deleteIfExists(buildDir);
            } catch (DirectoryNotEmptyException e) {
                throw new IllegalStateException(String.format("Build directory [%s] already exists and is non-empty. Check and empty directory before running command again",buildDir));
            }
            Files.createDirectories(buildDir);
            copyRelativeFile(scriptFile, rootDir, buildDir);
            if (graphQLSchemaFile.isPresent()) {
                Preconditions.checkArgument(Files.isRegularFile(graphQLSchemaFile.get()));
                copyFile(graphQLSchemaFile.get(), buildDir, Path.of(SCHEMA_FILE_NAME));
            }
            //Update dependencies and write out
            Path packageFile = buildDir.resolve(PACKAGE_FILE_NAME);
            mapper.writeValue(packageFile.toFile(),packageConfig);
            //Add external dependencies
            //TODO: implement
            Preconditions.checkArgument(config.getDependencies().isEmpty());
            //Recursively copy all files that can be handled by loaders
            DynamicLoader loader = new DynamicLoader();
            DynamicExporter exporter = new DynamicExporter();
            Predicate<Path> copyFilePredicate = path -> loader.usesFile(path) || exporter.usesFile(path);
            CopyFiles cpFiles = new CopyFiles(rootDir, buildDir, copyFilePredicate,
                    EXCLUDED_DIRS.stream().map(dir -> rootDir.resolve(dir)).collect(Collectors.toList()));
            Files.walkFileTree(rootDir, cpFiles);
        } catch (IOException e) {
            throw new IllegalStateException("Could not read or write files on local file-system", e);
        }
    }

    public void cleanUp() {
        try {
            Path buildDir = rootDir.resolve(BUILD_DIR_NAME);
            FileUtils.deleteDirectory(buildDir.toFile());
        } catch (IOException e) {
            throw new IllegalStateException("Could not read or write files on local file-system", e);
        }
    }

    @Value
    private class CopyFiles implements FileVisitor<Path> {

        Path srcDir;
        Path targetDir;
        Predicate<Path> copyFile;
        Collection<Path> excludedDirs;

        private boolean isExcludedDir(Path dir) throws IOException {
            for (Path p : excludedDirs) {
                if (dir.equals(p)) return true;
            }
            return false;
        }

        @Override
        public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
            if (isExcludedDir(dir)) return FileVisitResult.SKIP_SUBTREE;
            return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
            if (copyFile.test(file)) copyRelativeFile(file, srcDir, targetDir);
            return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
            //TODO: collect error
            return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
            return FileVisitResult.CONTINUE;
        }
    }

    public static void copyRelativeFile(Path srcFile, Path srcDir, Path destDir) throws IOException {
        copyFile(srcFile, destDir, srcDir.relativize(srcFile));
    }

    public static void copyFile(Path srcFile, Path destDir, Path relativeDestPath) throws IOException {
        Preconditions.checkArgument(Files.isRegularFile(srcFile));
        Path targetPath = destDir.resolve(relativeDestPath);
        Files.createDirectories(targetPath.getParent());
        Files.copy(srcFile, targetPath, StandardCopyOption.REPLACE_EXISTING);
//            targetPath.toFile().deleteOnExit();
    }


}
