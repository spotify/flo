package compiling;

import io.rouz.task.Task;
import io.rouz.task.cli.TaskConstructor;

import java.util.Arrays;
import java.util.Objects;

import javax.annotation.Generated;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpecBuilder;

@Generated("io.rouz.task.processor.TaskBindingProcessor")
public final class FloRootTaskFactory {

  private FloRootTaskFactory() {
    // no instantiation
  }

  public static TaskConstructor<String> ArgsTaskConstructor_Simple() {
    return new ArgsTaskConstructor_Simple();
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

  private static final class ArgsTaskConstructor_Simple implements TaskConstructor<String> {

    @Override
    public String name() {
      return "ArgsTaskConstructor.simple";
    }

    @Override
    public Task<String> create(String... args) {
      final OptionParser parser = parser();
      final OptionSet parse = parser.parse(args);

      final int a = (int) Objects.requireNonNull(parse.valueOf("a"));
      final double b = (double) Objects.requireNonNull(parse.valueOf("b"));
      final Integer c = (Integer) Objects.requireNonNull(parse.valueOf("c"));
      final Double d = (Double) Objects.requireNonNull(parse.valueOf("d"));
      final String e = (String) Objects.requireNonNull(parse.valueOf("e"));
      final boolean flag = parse.has("flag");

      return ArgsTaskConstructor.simple(a, b, c, d, e, flag);
    }

    public OptionParser parser() {
      final OptionParser parser = new OptionParser();

      opt("a", int.class, parser);
      opt("b", double.class, parser);
      opt("c", Integer.class, parser);
      opt("d", Double.class, parser);
      opt("e", String.class, parser);
      opt("flag", boolean.class, parser);

      parser.acceptsAll(Arrays.asList("h", "help")).forHelp();

      return parser;
    }
  }
}
