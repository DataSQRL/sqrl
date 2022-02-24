package ai.dataeng.sqml;

import ai.dataeng.sqml.config.util.NamedIdentifier;

import java.util.stream.Stream;

public interface EnvironmentPersistence {

        public void saveSubmission(ScriptSubmission script);

        public ScriptSubmission getSubmissionById(NamedIdentifier id);

        public Stream<ScriptSubmission> getAllSubmissions();

}
