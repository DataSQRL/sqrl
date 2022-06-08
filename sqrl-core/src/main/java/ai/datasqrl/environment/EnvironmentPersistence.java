package ai.datasqrl.environment;

import ai.datasqrl.config.util.NamedIdentifier;
import java.util.stream.Stream;

public interface EnvironmentPersistence {

  void saveDeployment(ScriptDeployment script);

  ScriptDeployment getSubmissionById(NamedIdentifier id);

  Stream<ScriptDeployment> getAllDeployments();

}
