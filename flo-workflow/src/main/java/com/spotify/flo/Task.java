/*-
 * -\-\-
 * Flo Workflow Definition
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

package com.spotify.flo;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

/**
 * TODO:
 * d make inputs lazily instantiated to allow short circuiting graph generation
 * d output task graph
 * . external outputs - outputs that might already be available (ie a file on disk)
 *   - can be implemented with a regular task, but better support can be added
 * . identity task
 *
 * @param <T>  A type carrying the execution metadata of this task
 */
@AutoValue
public abstract class Task<T> implements Serializable {

  public abstract TaskId id();

  public abstract Class<T> type();

  abstract EvalClosure<T> code();

  abstract Map<MetaKey<?>, Object> meta();

  abstract Fn<List<Task<?>>> lazyInputs();

  public abstract List<OpProvider<?>> ops();

  public List<Task<?>> inputs() {
    return lazyInputs().get();
  }

  public <E> E getMeta(MetaKey<E> key) {
    //noinspection unchecked
    return (E) meta().get(key);
  }

  public Stream<Task<?>> inputsInOrder() {
    return inputsInOrder(new HashSet<>());
  }

  private Stream<Task<?>> inputsInOrder(Set<TaskId> visits) {
    return inputs().stream()
        .filter(input -> !visits.contains(input.id()))
        .peek(input -> visits.add(input.id()))
        .flatMap(input -> Stream.concat(
            input.inputsInOrder(visits),
            Stream.of(input)
        ));
  }

  public static NamedTaskBuilder named(String taskName, Object... args) {
    return new NTB(TaskId.create(taskName, args));
  }

  public static <T> Task<T> create(Fn<T> code, Class<T> type, String taskName, Object... args) {
    return create(
        Collections.emptyMap(), Collections::emptyList, Collections.emptyList(),
        type,
        ec -> ec.value(code),
        TaskId.create(taskName, args));
  }

  static <T> Task<T> create(
      Map<MetaKey<?>, Object> meta,
      Fn<List<Task<?>>> inputs,
      List<OpProvider<?>> ops,
      Class<T> type,
      EvalClosure<T> code,
      TaskId taskId) {
    return new AutoValue_Task<>(taskId, type, code, meta, inputs, ops);
  }

  /**
   * This is only needed because a generic lambda can not be implemented
   * in {@link #named(String, Object...)}.
   */
  private static final class NTB implements NamedTaskBuilder {

    private final TaskId taskId;

    private NTB(TaskId taskId) {
      this.taskId = taskId;
    }

    @Override
    public <Z> TaskBuilder<Z> ofType(Class<Z> type) {
      return TaskBuilderImpl.rootBuilder(taskId, type);
    }
  }
}
