package io.rouz;

import com.google.auto.value.AutoValue;

import java.util.List;
import java.util.function.Function;

import static java.util.Collections.emptyList;

/**
 *
 * Task definitions have (TD)
 *  - a unique name
 *  - a list of parameters of specific types
 *  - an output type
 *
 * Task instances have (TI)
 *  - a TD
 *  - specific values for all TD parameters
 *  - a list of input TI
 *  - code for producing the output
 *
 *
 * Notes
 *  - the tricky part seems to lie in propagating depended task results to the dependent task
 *  --- main issue is in type safety, can easily be done with hashmaps
 *  - creating task instances should only yield a full dependency tree based on the task parameters
 *  - execution of tasks is subject to memoization
 *
 *  - can the setup, with dynamic dependencies be done with type-safe code?
 *  --- maybe with a value+trait based approach rather than classes+inheritance
 */
public class Scratch {

  public static void main(String[] args) {
    Task<String> task1 = MyTask.create("foo");
    Task<Integer> task2 = OtherTask.create(5, 7);
    MyTaskClass task3 = new MyTaskClass(42, "bar");

    System.out.println("task1.output() = " + task1.output(null));
    System.out.println("task2.output() = " + task2.output(null));
    System.out.println("task3.output() = " + task3.output(null));
  }

  static class MyTaskClass extends TaskBase<String> {

    private final int integer;
    private final String string;

    MyTaskClass(int integer, String string) {
      super(inputs(integer, string));
      this.integer = integer;
      this.string = string;
    }

    private static Input2<String, Integer> inputs(int integer, String string) {
      return Inputs
          .from(MyTask.create("foo " + string))
          .with(OtherTask.create(integer, 3));
    }

    @Override
    String output(TaskContext taskContext) {
      // can has dependent task outputs plz
      return integer + string;
    }
  }

  static abstract class TaskBase<T> {

    private final Inputs inputs;

    protected TaskBase(Inputs inputs, Object... params) {
      this.inputs = inputs;
    }

    Inputs inputs() {
      return inputs;
    }

    abstract T output(TaskContext taskContext);
  }

  interface Inputs {
    static <A> Input1<A> from(Task<A> taska) {
      return new Input1<A>() {
        @Override
        public <B> Input2<A, B> with(Task<B> taskb) {
          return new Input2<A, B>() {
            @Override
            public <C> Input3<A, B, C> with(Task<C> taskc) {
              return new Input3<A, B, C>() {
              };
            }
          };
        }
      };
    }
  }

  interface Input1<A> extends Inputs {
    <B> Input2<A, B> with(Task<B> task);
  }

  interface Input2<A, B> extends Inputs {
    <C> Input3<A, B, C> with(Task<C> task);
  }

  interface Input3<A, B, C> extends Inputs {
  }

  static class MyTask {
    static Task<String> create(String parameter) {
      return Tasks.create(context -> something(parameter));
    }

    static String something(String parameter) {
      return "hello world " + parameter;
    }
  }

  static class OtherTask {
    static Task<Integer> create(int a, int b) {
      return Tasks.create(context -> a + b);
    }
  }

  @AutoValue
  static abstract class Tasks<T> implements Task<T> {

    protected abstract Function<TaskContext, T> code();

    static <T> Task<T> create(Function<TaskContext, T> code) {
      return new AutoValue_Scratch_Tasks<>(emptyList(), emptyList(), code);
    }

    @Override
    public T output(TaskContext taskContext) {
      return code().apply(taskContext);
    }
  }

  interface Task<T> {
    List<Parameter> parameters();
    List<Task<?>> inputs();

    T output(TaskContext taskContext);
  }

  interface Parameter {
  }

  interface TaskContext {
    // should contain {@link Output}s of each input task
    <T extends Inputs> Values<T> values(T t);
  }

  interface Values<T extends Inputs> {
  }
}
