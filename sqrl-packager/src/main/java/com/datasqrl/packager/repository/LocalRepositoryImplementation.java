package com.datasqrl.packager.repository;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorPrefix;
import com.datasqrl.name.NamePath;
import com.datasqrl.packager.config.Dependency;
import com.datasqrl.packager.util.Zipper;
import com.datasqrl.util.FileUtil;
import com.datasqrl.util.NameUtil;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import net.lingala.zip4j.ZipFile;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

@AllArgsConstructor
@Slf4j
public class LocalRepositoryImplementation implements Repository, CacheRepository, PublishRepository {

    public static final String LOCAL_REPO_NAME = "datasqrl";

    @Getter
    private final Path repositoryPath;
    private final ErrorCollector errors;

    public static LocalRepositoryImplementation of(ErrorCollector errors) {
        errors = errors.withLocation(ErrorPrefix.CONFIG.resolve("local-repository"));
        try {
            return new LocalRepositoryImplementation(FileUtil.makeHiddenFolder(FileUtil.getUserRoot(), LOCAL_REPO_NAME), errors);
        } catch (IOException e) {
            errors.fatal("Could not write to local file system: %s", e);
            return null;
        }
    }

    @Override
    public boolean retrieveDependency(Path targetPath, Dependency dependency) throws IOException {
        Path zipFile = dependency2Path(dependency);
        if (Files.isRegularFile(zipFile)) {
            new ZipFile(zipFile.toFile()).extractAll(targetPath.toString());
            return true;
        }
        return false;
    }

    @Override
    public void cacheDependency(Path zipFile, Dependency dependency) throws IOException {
        Path destFile = dependency2Path(dependency);
        Path parentDir = destFile.getParent();
        if (!Files.isDirectory(parentDir)) Files.createDirectories(parentDir);
        Files.copy(zipFile, destFile);
    }

    /**
     * Local repository does not support resolving the "latest" version and variant for a package
     * in order to avoid returning stale results compared to remote repositories.
     *
     * @param packageName
     * @return
     */
    @Override
    public Optional<Dependency> resolveDependency(String packageName) {
        return Optional.empty();
    }

    private Path dependency2Path(Dependency dependency) {
        return package2Path(dependency.getName())
                .resolve(dependency.getVersion())
                .resolve(FileUtil.addExtension(dependency.getVariant(), Zipper.ZIP_EXTENSION));
    }

    private Path package2Path(String packageName) {
        NamePath pkg = NamePath.parse(packageName);
        return NameUtil.namepath2Path(repositoryPath,pkg);
    }

    @Override
    public void publish(Path zipFile, Dependency dependency) {
        try {
            cacheDependency(zipFile, dependency);
        } catch (IOException ex) {
            throw errors.handle(ex);
        }
    }
}
