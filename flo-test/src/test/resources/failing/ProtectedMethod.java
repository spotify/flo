package failing;

import com.spotify.flo.Task;
import com.spotify.flo.processor.RootTask;

class ProtectedMethod {

  @RootTask
  protected static Task<?> privateMethod() {
    return null;
  }
}
