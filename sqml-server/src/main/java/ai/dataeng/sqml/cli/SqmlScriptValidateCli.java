package ai.dataeng.sqml.cli;

import ai.dataeng.sqml.MetadataManager;
import ai.dataeng.sqml.Session;
import ai.dataeng.sqml.analyzer.Analysis;
import ai.dataeng.sqml.analyzer.ScriptAnalyzer;
import ai.dataeng.sqml.connector.Sink;
import ai.dataeng.sqml.connector.Source;
import ai.dataeng.sqml.materialization.GqlQuery;
import ai.dataeng.sqml.materialization.GqlQueryParser;
import ai.dataeng.sqml.materialization.Plan;
import ai.dataeng.sqml.materialization.ValidationSchema;
import ai.dataeng.sqml.metadata.Metadata;
import ai.dataeng.sqml.metadata.PostgresMetadataResolver;
import ai.dataeng.sqml.model.Model;
import ai.dataeng.sqml.sql.parser.ParsingOptions;
import ai.dataeng.sqml.sql.parser.ParsingOptions.DecimalLiteralTreatment;
import ai.dataeng.sqml.sql.parser.SqlParser;
import ai.dataeng.sqml.sql.tree.Script;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

@Command(name = "server", mixinStandardHelpOptions = true, version = "Sqml Server 1.0",
    description = "Runs an sqml server with all the fixings.")
public class SqmlScriptValidateCli implements Callable<Void> {
  ObjectMapper objectMapper = new YAMLMapper();

  @Parameters(index = "0", description = "The sqml directory.")
  private File dir;

  @Override
  public Void call() throws Exception {
    Preconditions.checkNotNull(dir, "Could not find sqml directory");
    Preconditions.checkState(dir.isDirectory(), "Argument should be a directory, not a file.");

    SqlParser sqlParser = new SqlParser();
    ParsingOptions parsingOptions = ParsingOptions.builder()
        .setDecimalLiteralTreatment(
            DecimalLiteralTreatment.AS_DOUBLE).build();

    Sink sink = createSink(dir.toPath().resolve("connectors").resolve("sink"));

    List<Source> sources = createSources(dir.toPath().resolve("connectors").resolve("source"));
    Script script = createScripts(dir.toPath(), sqlParser, parsingOptions);
    Plan plan = createPlan(dir.toPath().resolve("plan.yml"));
    ValidationSchema validationSchema = parseValidationSchema(dir.toPath().resolve("schema.yml"));
    Session session = sink.createSession();


    Metadata metadata = new MetadataManager(session, new PostgresMetadataResolver(session));;

    //1. Parse script

    ScriptAnalyzer scriptAnalyzer = new ScriptAnalyzer(session, sqlParser, null, metadata);
    Model model = scriptAnalyzer.analyze(script);

    return null;
  }

  private List<GqlQuery> parseQueries(Path path,
      GqlQueryParser parser) {
    return Arrays.stream(path.toFile().listFiles())
        .filter(f -> f.isFile() && f.toPath().toString().endsWith(".graphql"))
        .map(f-> {
          try {
            String file = Files.readString(f.toPath());
            return parser.parseQuery(file);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }).collect(Collectors.toList());
  }

  private ValidationSchema parseValidationSchema(Path path) throws IOException {
    return objectMapper.readValue(path.toFile(), ValidationSchema.class);
  }

  private Plan createPlan(Path path) throws IOException {
    return objectMapper.readValue(path.toFile(), Plan.class);
  }

  private Script createScripts(Path path, SqlParser sqlParser,
      ParsingOptions parsingOptions) {

    return Arrays.stream(path.toFile().listFiles())
        .filter(f->f.isFile() && f.toPath().toString().endsWith(".sqml"))
        .map(f-> {
          try {
            String file = Files.readString(f.toPath());
            return sqlParser.createScript(file, parsingOptions);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }).findFirst()
        .get();
  }

  private List<Source> createSources(Path path) {
    return Arrays.stream(path.toFile().listFiles())
        .map(f -> {
          try {
            return objectMapper.readValue(f, Source.class);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        })
        .collect(Collectors.toList());
  }

  private Sink createSink(Path path) throws IOException {
    Preconditions.checkNotNull(path, "Could not find connectors/sink directory");
    Preconditions.checkState(path.toFile().isDirectory(), "connectors/sink should be a directory, not a file.");
    Preconditions.checkState(path.toFile().listFiles().length == 1);

    return objectMapper.readValue(path.toFile().listFiles()[0], Sink.class);
  }

  public static void main(String... args) {
    int exitCode = new CommandLine(new SqmlScriptValidateCli()).execute(args);
  }
}