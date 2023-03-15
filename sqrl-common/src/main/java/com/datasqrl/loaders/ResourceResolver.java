package com.datasqrl.loaders;

import com.datasqrl.name.NamePath;

import java.net.URI;
import java.util.List;
import java.util.Optional;

public interface ResourceResolver {

  List<URI> loadPath(NamePath namePath);

  Optional<URI> resolveFile(NamePath namePath);
}
