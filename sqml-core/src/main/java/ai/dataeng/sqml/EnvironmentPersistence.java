package ai.dataeng.sqml;

import ai.dataeng.sqml.config.util.NamedIdentifier;
import java.util.stream.Stream;

public interface EnvironmentPersistence {

        public void saveDeployment(ScriptDeployment script);

        public ScriptDeployment getSubmissionById(NamedIdentifier id);

        public Stream<ScriptDeployment> getAllDeployments();

}
