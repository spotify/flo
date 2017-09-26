package compiling;

import com.spotify.flo.Task;
import com.spotify.flo.processor.RootTask;

class ArgsTaskConstructor {

  @RootTask
  static Task<String> simple(int a, double b, Integer c, Double d, String e, boolean flag) {
    return null;
  }
}
