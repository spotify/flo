package common.pkg;

import io.rouz.task.Task;

import common.pkg.sibling1.Sibling1;
import common.pkg.sibling2.Sibling2;

import java.lang.String;
import java.util.Map;

public final class NameMeFactory {

  private NameMeFactory() {
    // no instantiation
  }

  public static Task<String> simple1(Map<String, String> $args) {
    return Sibling1.simple1();
  }

  public static Task<String> simple2(Map<String, String> $args) {
    return Sibling2.simple2();
  }
}
