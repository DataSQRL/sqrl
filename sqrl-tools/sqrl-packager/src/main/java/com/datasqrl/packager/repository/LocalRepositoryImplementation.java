package com.datasqrl.packager.repository;

import com.datasqrl.config.Dependency;
import com.datasqrl.config.PackageConfiguration;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorPrefix;
import com.datasqrl.canonicalizer.NamePath;
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

    public static final String LOCAL_REPO_NAME = "datasqrl/repository";

    @Getter
    private final Path repositoryPath;
    private final Path rootDir;
    private final ErrorCollector errors;

    public static LocalRepositoryImplementation of(ErrorCollector errors, Path rootDir) {
        errors = errors.withLocation(ErrorPrefix.CONFIG.resolve("local-repository"));
        try {
            return new LocalRepositoryImplementation(FileUtil.makeHiddenFolder(FileUtil.getUserRoot(), LOCAL_REPO_NAME), rootDir, errors);
        } catch (IOException e) {
            errors.fatal("Could not write to local file system: %s", e);
            return null;
        }
    }

    @Override
    public boolean retrieveDependency(Path targetPath, Dependency dependency) throws IOException {
        Path zipFile = getZipFilePath(dependency);
        if (Files.isRegularFile(zipFile)) {
            try (ZipFile zip = new ZipFile(zipFile.toFile())) {
                zip.extractAll(targetPath.toString());
            }
            return true;
        }
        return false;
    }

    @Override
    public void cacheDependency(Path zipFile, Dependency dependency) throws IOException {
        Path destFile = dependency2Path(dependency)
            .resolve(FileUtil.addExtension(dependency.getVariant(), Zipper.ZIP_EXTENSION));
        Path parentDir = destFile.getParent();
        if (!Files.isDirectory(parentDir)) Files.createDirectories(parentDir);
        Files.deleteIfExists(destFile);
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

    public Path getZipFilePath(Dependency dependency) {
        Path path = dependency2Path(dependency);
        Path zipFile = path.resolve(FileUtil.addExtension(dependency.getVariant(), Zipper.ZIP_EXTENSION));
        return zipFile;
    }

    private Path dependency2Path(Dependency dependency) {
        return package2Path(dependency.getName())
                .resolve(dependency.getVersion().get());
    }

    private Path package2Path(String packageName) {
        NamePath pkg = NamePath.parse(packageName);
        return NameUtil.namepath2Path(repositoryPath,pkg);
    }

    @Override
    public boolean publish(Path zipFile, PackageConfiguration pkgConfig) {
        pkgConfig.checkInitialized();
        Dependency dependency = pkgConfig.asDependency();
        try {
            cacheDependency(zipFile, dependency);
            return true;
        } catch (IOException ex) {
            throw errors.handle(ex);
        }
    }
}
