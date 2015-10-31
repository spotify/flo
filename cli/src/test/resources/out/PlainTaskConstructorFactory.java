package compiling;

import io.rouz.task.Task;

import java.lang.String;
import java.util.Map;

public final class NameMeFactory {

  private NameMeFactory() {
    // no instantiation
  }

  public static Task<String> simple(Map<String, String> $args) {
    return PlainTaskConstructor.simple();
  }
}
