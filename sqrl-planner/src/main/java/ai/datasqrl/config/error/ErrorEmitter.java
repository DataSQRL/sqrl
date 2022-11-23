package ai.datasqrl.config.error;

import ai.datasqrl.parse.tree.name.Name;
import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
public class ErrorEmitter {

  @Getter
  private final ErrorLocation baseLocation;

  public ErrorEmitter resolve(Name location) {
    return new ErrorEmitter(baseLocation.resolve(location));
  }
  public ErrorEmitter resolve(String location) {
    return new ErrorEmitter(baseLocation.resolve(location));
  }

  public ErrorMessage fatal(String msg, Object... args) {
    return new ErrorMessage.Implementation(getMessage(msg, args), baseLocation,
        ErrorMessage.Severity.FATAL);
  }

  public ErrorMessage fatal(int line, int offset, String msg, Object... args) {
    return new ErrorMessage.Implementation(getMessage(msg, args), baseLocation.atFile(line, offset),
        ErrorMessage.Severity.FATAL);
  }

  public ErrorMessage warn(String msg, Object... args) {
    return new ErrorMessage.Implementation(getMessage(msg, args), baseLocation,
        ErrorMessage.Severity.WARN);
  }

  public ErrorMessage warn(int line, int offset, String msg, Object... args) {
    return new ErrorMessage.Implementation(getMessage(msg, args), baseLocation.atFile(line, offset),
        ErrorMessage.Severity.WARN);
  }

  public ErrorMessage notice(String msg, Object... args) {
    return new ErrorMessage.Implementation(getMessage(msg, args), baseLocation,
        ErrorMessage.Severity.NOTICE);
  }

  public ErrorMessage notice(int line, int offset, String msg, Object... args) {
    return new ErrorMessage.Implementation(getMessage(msg, args), baseLocation.atFile(line, offset),
        ErrorMessage.Severity.NOTICE);
  }

  private static String getMessage(String msgTemplate, Object... args) {
    if (args == null || args.length == 0) {
      return msgTemplate;
    }
    return String.format(msgTemplate, args);
  }
}
