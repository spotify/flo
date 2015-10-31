package common.pkg.sibling1;

import io.rouz.task.Task;
import io.rouz.task.processor.RootTask;

public class Sibling1 {

  @RootTask
  public static Task<String> simple1() {
    return null;
  }
}
