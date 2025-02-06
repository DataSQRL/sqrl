package com.datasqrl.packager.repository;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.config.Dependency;
import com.datasqrl.config.PackageConfiguration;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorPrefix;
import com.datasqrl.packager.util.Zipper;
import com.datasqrl.util.FileUtil;
import com.datasqrl.util.NameUtil;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import net.lingala.zip4j.ZipFile;

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
        var zipFile = getZipFilePath(dependency);
        if (Files.isRegularFile(zipFile)) {
            new ZipFile(zipFile.toFile()).extractAll(targetPath.toString());
            return true;
        }
        return false;
    }

    @Override
    public void cacheDependency(Path zipFile, Dependency dependency) throws IOException {
        var destFile = dependency2Path(dependency)
            .resolve(FileUtil.addExtension(dependency.getVariant(), Zipper.ZIP_EXTENSION));
        var parentDir = destFile.getParent();
        if (!Files.isDirectory(parentDir)) {
			Files.createDirectories(parentDir);
		}
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
        var path = dependency2Path(dependency);
        var zipFile = path.resolve(FileUtil.addExtension(dependency.getVariant(), Zipper.ZIP_EXTENSION));
        return zipFile;
    }

    private Path dependency2Path(Dependency dependency) {
        return package2Path(dependency.getName())
                .resolve(dependency.getVersion().get());
    }

    private Path package2Path(String packageName) {
        var pkg = NamePath.parse(packageName);
        return NameUtil.namepath2Path(repositoryPath,pkg);
    }

    @Override
    public boolean publish(Path zipFile, PackageConfiguration pkgConfig) {
        pkgConfig.checkInitialized();
        var dependency = pkgConfig.asDependency();
        try {
            cacheDependency(zipFile, dependency);
            return true;
        } catch (IOException ex) {
            throw errors.handle(ex);
        }
    }
}
