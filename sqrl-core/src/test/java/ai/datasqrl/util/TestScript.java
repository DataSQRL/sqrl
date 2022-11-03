package ai.datasqrl.util;

import java.nio.file.Path;

public interface TestScript {

    String getName();

    Path getRootPackageDirectory();

    Path getScript();

}
