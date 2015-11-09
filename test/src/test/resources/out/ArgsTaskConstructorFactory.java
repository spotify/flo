package compiling;

import io.rouz.task.Task;
import io.rouz.task.cli.TaskConstructor;

import java.util.Arrays
import java.util.Map;

import javax.annotation.Generated;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpecBuilder;

import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;

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

      if (parse.has("h")) {
        parser.printHelpOn(System.err);
        System.exit(1); // maybe not exit
      }

      final int a = (int) requireNonNull(parse.valueOf("a"));
      final double b = (double) requireNonNull(parse.valueOf("b"));
      final int c = (int) requireNonNull(parse.valueOf("c"));
      final double d = (double) requireNonNull(parse.valueOf("d"));
      final String e = (String) requireNonNull(parse.valueOf("e"));;

      return ArgsTaskConstructor.simple(a, b, c, d, e);
    }

    @Override
    public OptionParser parser() {
      final OptionParser parser = new OptionParser();

      // opt();

      parser.acceptsAll(Arrays.asList("h", "help")).forHelp();

      return parser;
    }
  }
}
