package compiling;

import io.rouz.flo.Task;
import io.rouz.flo.processor.RootTask;

class ArgsTaskConstructor {

  @RootTask
  static Task<String> simple(int a, double b, Integer c, Double d, String e, boolean flag) {
    return null;
  }
}
