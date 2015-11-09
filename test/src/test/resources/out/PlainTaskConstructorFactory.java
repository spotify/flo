package compiling;

import io.rouz.task.Task;
import io.rouz.task.cli.TaskConstructor;

import java.util.Map;

import javax.annotation.Generated;

import joptsimple.OptionParser;
import joptsimple.OptionSpecBuilder;

@Generated("io.rouz.task.processor.TaskBindingProcessor")
public final class FloRootTaskFactory {

  private FloRootTaskFactory() {
    // no instantiation
  }

  public static Task<String> simple(Map<String, String> $args) {
    return PlainTaskConstructor.simple();
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

  private static final class PlainTaskConstructor_Simple implements TaskConstructor<String> {

    @Override
    public String name() {
      return "PlainTaskConstructor.simple";
    }

    @Override
    public Task<String> create(String... args) {
      return PlainTaskConstructor.simple();
    }

    @Override
    public OptionParser parser() {
      final OptionParser parser = new OptionParser();
      return parser;
    }
  }
}
