import io.rouz.task.processor.RootTask;

class Test {

  @RootTask
  static String notTask() {
    return "I should be a Task<?>";
  }
}
