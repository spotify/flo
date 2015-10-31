package compiling;

import io.rouz.task.Task;

import java.lang.Double;
import java.lang.Integer;
import java.lang.String;
import java.util.Map;

import javax.annotation.Generated;

@Generated("io.rouz.task.processor.TaskBindingProcessor")
public final class NameMeFactory {

  private NameMeFactory() {
    // no instantiation
  }

  public static Task<String> simple(Map<String, String> $args) {
    int a = Integer.parseInt($args.get("a"));
    double b = Double.parseDouble($args.get("b"));
    int c = Integer.parseInt($args.get("c"));
    double d = Double.parseDouble($args.get("d"));
    String e = $args.get("e");
    return ArgsTaskConstructor.simple(a, b, c, d, e);
  }
}
