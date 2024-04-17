package com.datasqrl.cmd;

import static com.datasqrl.packager.Packager.DEFAULT_PACKAGE;

import com.datasqrl.config.Dependency;
import com.datasqrl.config.PackageJsonImpl;
import com.datasqrl.config.SqrlConfigCommons;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.NotYetImplementedException;
import com.datasqrl.packager.Packager;
import com.datasqrl.packager.Publisher;
import com.datasqrl.packager.repository.LocalRepositoryImplementation;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import picocli.CommandLine;

@CommandLine.Command(name = "publish", description = "Publishes a package to local and remote repository")
public class PublishCommand extends AbstractCommand {

    @CommandLine.Parameters(index = "0", description = "Main script (optional)")
    private Path mainScript;

    @CommandLine.Option(names = {"--remote"}, description = "Publish to remote repository (local only by default)")
    private boolean toRemote = false;

    @Override
    protected void execute(ErrorCollector errors) throws IOException {
        if (toRemote) NotYetImplementedException.trigger("Publishing to remote repository is not yet supported");
        Path packageRoot = root.rootDir;
        Optional<List<Path>> packageConfigsOpt = Packager.findPackageFile(root.rootDir, root.packageFiles);
        errors.checkFatal(packageConfigsOpt.isPresent(),"Directory does not contain [%s] package configuration file",
            Packager.PACKAGE_JSON);
        List<Path> packageconfigs = packageConfigsOpt.get();
        Path defaultPkgConfig = packageRoot.resolve(DEFAULT_PACKAGE);

        LocalRepositoryImplementation localRepo = LocalRepositoryImplementation.of(errors,
            root.rootDir);
        Publisher publisher = new Publisher(errors);

        if (mainScript==null && packageconfigs.size()==1 && Files.isSameFile(defaultPkgConfig,packageconfigs.get(0))
                && !new PackageJsonImpl(SqrlConfigCommons.fromFiles(errors,defaultPkgConfig)).hasScriptKey()) {
            //If no main script is specified and only a single default package config and that config does not contain a script config
            //then we are publishing a data source/sink or function package (i.e. we don't need to build)
            Dependency dep = publisher.publish(packageRoot, localRepo).asDependency();
            System.out.println(String.format("Successfully published package [%s] to local repository", dep));
        } else {
            //We are publishing a script bundle and need to build before invoking the publisher on the build directory
            NotYetImplementedException.trigger("Publishing SQRL scripts is not yet supported");
        }

    }

}
