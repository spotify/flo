package io.rouz.flo.freezer;

import static io.rouz.flo.freezer.LoggingListener.colored;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.ClosureSerializer;
import io.rouz.flo.Fn;
import io.rouz.flo.Task;
import io.rouz.flo.TaskContext;
import io.rouz.flo.TaskId;
import io.rouz.flo.context.ForwardingTaskContext;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import org.objenesis.strategy.StdInstantiatorStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link TaskContext} that serializes and persist tasks. Any call to {@link #evaluate(Task)}
 * will persist the task and recurse to also do so for all input tasks. No task in the dependency
 * tree will actually be invoked. Instead {@link #evaluate(Task)} will return a {@link Value} that
 * always fails.
 *
 * <p>After the returned {@link Value} has failed, all persisted file paths can be received through
 * {@link #getFiles()}.
 */
public class PersistingContext extends ForwardingTaskContext {

  private static final Logger LOG = LoggerFactory.getLogger(PersistingContext.class);

  private final Path basePath;
  private final Map<TaskId, Path> files = new LinkedHashMap<>();

  public PersistingContext(Path basePath, TaskContext delegate) {
    super(delegate);
    this.basePath = Objects.requireNonNull(basePath);
  }

  public Map<TaskId, Path> getFiles() {
    return files;
  }

  @Override
  public <T> Value<T> evaluateInternal(Task<T> task, TaskContext context) {
    // materialize lazy inputs
    task.inputs();

    Path file = taskFile(task.id());
    files.put(task.id(), file);
    try {
      serialize(task, file);
    } catch (Exception e) {
      e.printStackTrace();
    }

    return super.evaluateInternal(task, context);
  }

  @Override
  public <T> Value<T> invokeProcessFn(TaskId taskId, Fn<Value<T>> processFn) {
    final Promise<T> promise = promise();
    LOG.info("Will not invoke {}", colored(taskId));
    promise.fail(new RuntimeException("will not invoke"));
    return promise.value();
  }

  public static void serialize(Object object, Path file) throws Exception{
    final Kryo kryo = new Kryo();
    kryo.register(java.lang.invoke.SerializedLambda.class);
    kryo.register(ClosureSerializer.Closure.class, new ClosureSerializer());

    final File outputFil = file.toFile();
    if (!outputFil.createNewFile()) {
      throw new RuntimeException("File " + file + " already exists");
    }

    try {
      try (Output output = new Output(new FileOutputStream(outputFil))) {
        kryo.writeClassAndObject(output, object);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static <T> T deserialize(Path filePath) throws Exception {
    Kryo kryo = new Kryo();
    kryo.register(java.lang.invoke.SerializedLambda.class);
    kryo.register(ClosureSerializer.Closure.class, new ClosureSerializer());
    kryo.setInstantiatorStrategy(new Kryo.DefaultInstantiatorStrategy(new StdInstantiatorStrategy()));

    try (InputStream inputStream = new FileInputStream(filePath.toFile())) {
      Input input = new Input(inputStream);
      return (T) kryo.readClassAndObject(input);
    }
  }

  public static String cleanForFilename(TaskId taskId) {
    return taskId.toString()
        .toLowerCase()
        .replaceAll("[,#()]+", "_")
        .replaceAll("[^a-z0-9_]*", "")
    ;
  }

  private Path taskFile(TaskId taskId) {
    return basePath.resolve(cleanForFilename(taskId));
  }
}
