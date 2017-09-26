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

/**
 * An identifier that uniquely identifies a Task Instance
 */
public interface TaskId {

  /**
   * @return The name of the task
   */
  String name();

  /**
   * @return A hash for the task parameter values
   */
  int hash();

  /**
   * Creates a {@link TaskId}.
   *
   * <p>The task name can not contain any parenthesis.
   *
   * @throws IllegalArgumentException if the name contains any parenthesis
   * @param name  Task name
   * @param args  Arbitrary task arguments
   * @return A task id for the given arguments
   */
  static TaskId create(String name, Object... args) {
    return TaskIds.create(name, args);
  }

  /**
   * Parses a {@link TaskId} based on a string representation.
   *
   * <p>A parsed id is equal to a task id that was created using {@link #create(String, Object...)}.
   *
   * @throws IllegalArgumentException if the string does not conform to a task id
   * @param stringId  The id to parse
   * @return A parsed task id
   */
  static TaskId parse(String stringId) {
    return TaskIds.parse(stringId);
  }
}
