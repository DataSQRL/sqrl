package com.datasqrl.packager.repository;

import com.datasqrl.packager.config.Dependency;
import java.util.regex.Pattern;
import lombok.AllArgsConstructor;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

@AllArgsConstructor
public class CompositeRepositoryImpl implements Repository {

    private final List<Repository> repositories;

    @Override
    public boolean retrieveDependency(Path targetPath, Dependency dependency) throws IOException {
        for (Repository rep: repositories) {
            if (rep.retrieveDependency(targetPath, dependency)) return true;
        }
        return false;
    }

    @Override
    public Optional<Dependency> resolveDependency(String packageName) {
        if (!packageName.contains(".")) {
            return Optional.empty();
        }
        return repositories.stream().map(rep -> rep.resolveDependency(packageName))
                .filter(Optional::isPresent).findFirst().orElse(Optional.empty());
    }
}
