package com.datasqrl.loaders;

import com.datasqrl.canonicalizer.NamePath;
import lombok.Value;

/**
 * Information about the module attempted to be loaded
 */
@Value
public class ModuleMetadata {
  NamePath name;
  ObjectLoaderMetadata objectLoaderMetadata;
}
