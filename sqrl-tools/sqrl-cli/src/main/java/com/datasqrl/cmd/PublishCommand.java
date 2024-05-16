package com.datasqrl.cmd;

import static com.datasqrl.packager.Packager.DEFAULT_PACKAGE;

import com.datasqrl.config.PackageConfiguration;
import com.datasqrl.config.PackageJsonImpl;
import com.datasqrl.config.SqrlConfigCommons;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.NotYetImplementedException;
import com.datasqrl.packager.Packager;
import com.datasqrl.packager.Publisher;
import com.datasqrl.packager.repository.LocalRepositoryImplementation;
import com.datasqrl.packager.repository.RemoteRepositoryImplementation;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import picocli.CommandLine;

@CommandLine.Command(name = "publish", description = "Publishes a package to local and remote repository")
@Slf4j
public class PublishCommand extends AbstractCommand {

  @CommandLine.Option(names = {"--local"}, description = "Publish to local repository only")
  private boolean toLocal = false;

  @Override
  protected void execute(ErrorCollector errors) throws IOException {
    Path packageRoot = root.rootDir;
    Optional<List<Path>> packageConfigsOpt = Packager.findPackageFile(root.rootDir,
        root.packageFiles);
    errors.checkFatal(packageConfigsOpt.isPresent(),
        "Directory does not contain [%s] package configuration file", Packager.PACKAGE_JSON);
    List<Path> packageconfigs = packageConfigsOpt.get();
    Path defaultPkgConfig = packageRoot.resolve(DEFAULT_PACKAGE);

    LocalRepositoryImplementation localRepo = LocalRepositoryImplementation.of(errors,
        root.rootDir);
    Publisher publisher = new Publisher(errors);

    errors.checkFatal(packageconfigs.size() == 1 && Files.isSameFile(defaultPkgConfig,
        packageconfigs.get(0)), "Expecting a single package.json file for the package to be published");

    PackageConfiguration pkgConfig = publisher.publish(packageRoot, localRepo);
    if (!toLocal) {
      Path cachedPath = localRepo.getZipFilePath(pkgConfig.asDependency());
      RemoteRepositoryImplementation remoteRepo = new RemoteRepositoryImplementation();
      if (remoteRepo.publish(cachedPath, pkgConfig)) {
        log.info("Successfully published package [{}] to remote repository",
            pkgConfig.asDependency());
      }
    } else {
      log.info("Successfully published package [{}] to local repository",
          pkgConfig.asDependency());
    }

  }

}
