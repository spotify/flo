package com.spotify.flo.context;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import com.google.common.collect.ImmutableMap;
import com.spotify.flo.EvalContext;
import com.spotify.flo.Operation;
import com.spotify.flo.Task;
import com.spotify.flo.TaskBuilder.F1;
import com.spotify.flo.TaskContextStrict;
import com.spotify.flo.TaskId;
import com.spotify.flo.TaskOperator.Listener;
import com.spotify.flo.context.Jobs.JobSpec;
import com.spotify.flo.freezer.EvaluatingContext;
import com.spotify.flo.freezer.PersistingContext;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PersistedExecutionTest {

  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Mock private Listener listener;

  @Test
  public void test() throws Exception {


    final Task<String> foo = Task.named("foo")
        .ofType(String.class)
        .process(() -> {
          System.err.println("FOO");
          return "foo";
        });

    final Task<String> bar = Task.named("bar")
        .ofType(String.class)
        .process(() -> {
          System.err.println("BAR");
          return "bar";
        });

    final Task<String> quux = Task.named("quux")
        .ofType(String.class)
        .context(new DoneOutput<>("quux"))
        .process(s -> {
          throw new AssertionError("execution not expected");
        });

    final Task<String> baz = Task.named("baz")
        .ofType(String.class)
        .context(Jobs.JobOperator.create())
        .input(() -> foo)
        .input(() -> bar)
        .process((spec, a, b) -> {
          System.err.println("BAZ");
          return spec
              .options(() -> ImmutableMap.of("foo", "bar"))
              .pipeline(ctx -> ctx
                  .readFrom("foo")
                  .map("_ + _")
                  .writeTo("bar"))
              .validation(r -> {
                if (r.records == 0) {
                  throw new AssertionError("no records seen!");
                }
              })
              .success(r ->  a + " " + b);
        });

    final Task<String> root = Task.named("root")
        .ofType(String.class)
        .context(Jobs.JobOperator.create())
        .input(() -> foo)
        .input(() -> bar)
        .input(() -> baz)
        .input(() -> quux)
        .process((spec, a, b, c, d) -> {
          System.err.println("MAIN");
          return spec
              .options(() -> ImmutableMap.of("foo", "bar"))
              .pipeline(ctx -> ctx
                  .readFrom("foo")
                  .map("_ + _")
                  .writeTo("bar"))
              .validation(r -> {
                if (r.records == 0) {
                  throw new AssertionError("no records seen!");
                }
              })
              .success(r -> String.join(" ", a, b, c, d));
        });

    final Path persistedTasksDir = temporaryFolder.newFolder().toPath();
    final PersistingContext persistingContext = new PersistingContext(
        persistedTasksDir, EvalContext.sync());

    MemoizingContext.composeWith(persistingContext)
        .evaluate(root).toFuture().exceptionally(t -> "").get();
    final Map<TaskId, Path> persistedTasks = persistingContext.getFiles();

    // TODO: persist list of pending tasks
    final Set<Task<?>> pending = new HashSet<>();
    tasks(root, pending::add);

    final EvaluatingContext evaluatingContext = new EvaluatingContext(persistedTasksDir, EvalContext.sync());
    while (!pending.isEmpty()) {
      final Iterator<Task<?>> it = pending.iterator();
      while (it.hasNext()) {
        final Task<?> task = it.next();
        final boolean inputReady = task.inputs().stream().map(Task::id)
            .allMatch(evaluatingContext::isEvaluated);
        if (!inputReady) {
          continue;
        }

        it.remove();

        final Path path = persistedTasks.get(task.id());

        // Check if output has already been produced and execution can be skipped
        // TODO: Use EC delegation instead?
        @SuppressWarnings("unchecked") final Optional<TaskContextStrict<?, Object>> outputContext =
            (Optional) TaskContextStrict.taskContextStrict(task);
        if (outputContext.isPresent()) {
          @SuppressWarnings("unchecked") final Optional<Object> lookup = outputContext.get().lookup((Task<Object>) task);
          if (lookup.isPresent()) {
            evaluatingContext.persist(task.id(), lookup.get());
            continue;
          }
        }

        // Execute task
        if (OperationExtractingContext.operator(task).isPresent()) {
          // Perform operation
          final Operation operation = OperationExtractingContext.extract(task, taskOutput(evaluatingContext));
          // TODO: run operation in async manner
          final String output = ((JobSpec) operation).run(listener);
          evaluatingContext.persist(task.id(), output);
        } else {
          // Execute generic task
          evaluatingContext.evaluateTaskFrom(path).toFuture().get();
        }
      }
    }

    final String result = evaluatingContext.readExistingOutput(root.id());

    System.err.println("result: " + result);

    assertThat(result, is("foo bar foo bar quux"));
  }

  private F1<TaskId, ?> taskOutput(EvaluatingContext evaluatingContext) {
    return taskId -> {
      try {
        return evaluatingContext.readExistingOutput(taskId);
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    };
  }

  private void tasks(Task<?> task, Consumer<Task<?>> f) {
    f.accept(task);
    task.inputs().forEach(upstream -> tasks(upstream, f));
  }

  private static class DoneOutput<T> extends TaskContextStrict<String, T> {

    private final T value;

    public DoneOutput(T value) {
      this.value = value;
    }

    @Override
    public String provide(EvalContext evalContext) {
      return "";
    }

    @Override
    public Optional<T> lookup(Task<T> task) {
      return Optional.of(value);
    }
  }
}
