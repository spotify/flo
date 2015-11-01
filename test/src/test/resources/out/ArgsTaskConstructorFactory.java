package compiling;

import io.rouz.task.Task;

import java.util.Map;

import javax.annotation.Generated;

@Generated("io.rouz.task.processor.TaskBindingProcessor")
public final class FloRootTaskFactory {

  private FloRootTaskFactory() {
    // no instantiation
  }

  public static Task<String> simple(Map<String, String> $args) {
    final int a = Integer.parseInt($args.get("a"));
    final double b = Double.parseDouble($args.get("b"));
    final int c = Integer.parseInt($args.get("c"));
    final double d = Double.parseDouble($args.get("d"));
    final String e = $args.get("e");
    return ArgsTaskConstructor.simple(a, b, c, d, e);
  }
}
