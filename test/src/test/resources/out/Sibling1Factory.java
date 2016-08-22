package common.pkg.sibling1;

import io.rouz.flo.Task;

import java.util.Map;

import javax.annotation.Generated;

import joptsimple.OptionParser;
import joptsimple.OptionSpecBuilder;

@Generated("io.rouz.flo.processor.TaskBindingProcessor")
public final class FloRootTaskFactory {

  private FloRootTaskFactory() {
    // no instantiation
  }

  public static Task<String> simple1(Map<String, String> $args) {
    return Sibling1.simple1();
  }

  private static void opt(String name, Class type, OptionParser parser) {
    final boolean isFlag = boolean.class.equals(type);
    final OptionSpecBuilder spec = (isFlag)
                                   ? parser.accepts(name, "(default: false)")
                                   : parser.accepts(name);

    if (!isFlag) {
      spec.withRequiredArg().ofType(type).describedAs(name).required();
    }
  }
}
