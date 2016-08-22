package common.pkg.sibling1;

import io.rouz.flo.Task;
import io.rouz.flo.processor.RootTask;

public class Sibling1 {

  @RootTask
  public static Task<String> simple1() {
    return null;
  }
}
