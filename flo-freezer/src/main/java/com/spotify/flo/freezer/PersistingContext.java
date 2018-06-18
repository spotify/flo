/*-
 * -\-\-
 * flo-freezer
 * --
 * Copyright (C) 2016 - 2017 Spotify AB
 * --
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * -/-/-
 */

package com.spotify.flo.freezer;

import static java.nio.file.Files.newInputStream;
import static java.nio.file.Files.newOutputStream;
import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.nio.file.StandardOpenOption.WRITE;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.ClosureSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import com.spotify.flo.EvalContext;
import com.spotify.flo.Fn;
import com.spotify.flo.Task;
import com.spotify.flo.TaskId;
import com.spotify.flo.context.ForwardingEvalContext;
import com.twitter.chill.java.PackageRegistrar;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import org.objenesis.strategy.StdInstantiatorStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link EvalContext} that serializes and persist tasks. Any call to {@link #evaluate(Task)}
 * will persist the task and recurse to also do so for all input tasks. No task in the dependency
 * tree will actually be invoked. Instead {@link #evaluate(Task)} will return a {@link Value} that
 * always fails.
 *
 * <p>After the returned {@link Value} has failed, all persisted file paths can be received through
 * {@link #getFiles()}.
 */
public class PersistingContext extends ForwardingEvalContext {

  private static final Logger LOG = LoggerFactory.getLogger(PersistingContext.class);

  private final Path basePath;
  private final Map<TaskId, Path> files = new LinkedHashMap<>();

  public PersistingContext(Path basePath, EvalContext delegate) {
    super(delegate);
    this.basePath = Objects.requireNonNull(basePath);
  }

  public Map<TaskId, Path> getFiles() {
    return files;
  }

  @Override
  public <T> Value<T> evaluateInternal(Task<T> task, EvalContext context) {
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
  public <T> Value<T> invokeProcessFn(TaskId taskId, Fn<T> processFn) {
    final Promise<T> promise = promise();
    LOG.info("Will not invoke {}", taskId);
    promise.fail(new Persisted());
    return promise.value();
  }

  public static void serialize(Object object, Path file) throws Exception {
    try {
      serialize(object, Files.newOutputStream(file, WRITE, CREATE_NEW));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static void serialize(Object object, OutputStream outputStream) {
    final Kryo kryo = getKryo();

    try (Output output = new Output(outputStream)) {
      kryo.writeClassAndObject(output, object);
    }
  }

  public static <T> T deserialize(Path filePath) throws Exception {
    return deserialize(Files.newInputStream(filePath));
  }

  public static <T> T deserialize(InputStream inputStream) {
    final Kryo kryo = getKryo();

    try (Input input = new Input(inputStream)) {
      return (T) kryo.readClassAndObject(input);
    }
  }

  private static Kryo getKryo() {
    Kryo kryo = new Kryo();
    PackageRegistrar.all().apply(kryo);
    kryo.register(java.lang.invoke.SerializedLambda.class);
    kryo.register(ClosureSerializer.Closure.class, new ClosureSerializer());
    kryo.setInstantiatorStrategy(new Kryo.DefaultInstantiatorStrategy(new StdInstantiatorStrategy()));
    kryo.addDefaultSerializer(Throwable.class, new JavaSerializer());
    kryo.getFieldSerializerConfig().setIgnoreSyntheticFields(false);
    return kryo;
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
