package failing;

import com.spotify.flo.processor.RootTask;

class MethodNotReturningTask {

  @RootTask
  static String notTask() {
    return "I should be a Task<?>";
  }
}
