package compiling;

import io.rouz.task.Task;
import io.rouz.task.processor.RootTask;

class ArgsTaskConstructor {

  @RootTask
  static Task<String> simple(int a, double b, Integer c, Double d, String e) {
    return null;
  }
}
