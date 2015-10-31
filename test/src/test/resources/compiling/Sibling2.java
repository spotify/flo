package common.pkg.sibling2;

import io.rouz.task.Task;
import io.rouz.task.processor.RootTask;

public class Sibling2 {

  @RootTask
  public static Task<String> simple2() {
    return null;
  }
}
