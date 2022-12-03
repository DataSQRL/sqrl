package ai.datasqrl.parse;

import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;

public class LexerErrorHandler extends BaseErrorListener {
  @Override
  public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line,
      int charPositionInLine, String message, RecognitionException e) {
    throw new ParsingException(message, e, line, charPositionInLine);
  }
}
