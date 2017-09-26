package compiling;

import com.spotify.flo.Task;
import com.spotify.flo.processor.RootTask;

class PlainTaskConstructor {

  @RootTask
  static Task<String> simple() {
    return null;
  }
}
