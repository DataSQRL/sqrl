package ai.datasqrl.config.error;

public interface SourceMap {

  String getSource();

  public class EmptySourceMap implements SourceMap{

    @Override
    public String getSource() {
      return "";
    }
  }
}
