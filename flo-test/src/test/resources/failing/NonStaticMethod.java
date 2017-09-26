package failing;

import com.spotify.flo.Task;
import com.spotify.flo.processor.RootTask;

class NonStaticMethod {

  @RootTask
  Task<?> instanceMethod() {
    return null;
  }
}
