package ai.datasqrl.compile.loaders;

import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.plan.local.generate.Resolve;
import lombok.AllArgsConstructor;
import lombok.Value;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

@Value
@AllArgsConstructor
public class CompositeLoader implements Loader {

    List<Loader> loaders;

    public CompositeLoader(Loader... loaders) {
        this(List.of(loaders));
    }

    @Override
    public boolean load(Resolve.Env env, NamePath fullPath, Optional<Name> alias) {
        for (Loader loader : loaders) {
            if (loader.load(env,fullPath,alias)) return true;
        }
        return false;
    }

    @Override
    public Set<Name> loadAll(Resolve.Env env, NamePath basePath) {
        Set<Name> allLoaded = new HashSet<>();
        for (Loader loader : loaders) {
            allLoaded.addAll(loader.loadAll(env,basePath));
        }
        return allLoaded;
    }
}
