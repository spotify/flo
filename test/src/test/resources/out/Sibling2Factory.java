package common.pkg.sibling2;

import io.rouz.task.Task;

import java.util.Map;

import javax.annotation.Generated;

@Generated("io.rouz.task.processor.TaskBindingProcessor")
public final class NameMeFactory {

  private NameMeFactory() {
    // no instantiation
  }

  public static Task<String> simple2(Map<String, String> $args) {
    return Sibling2.simple2();
  }
}
