/*-
 * -\-\-
 * flo runner
 * --
 * Copyright (C) 2016 Spotify AB
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

package com.spotify.flo.status;

/**
 * Generic exception for signalling that a task could not execute for some reason.
 */
public abstract class TaskStatusException extends RuntimeException {

  private final int code;

  public TaskStatusException(int code) {
    super("Task failed with status code " + code);
    this.code = code;
  }

  public int code() {
    return code;
  }
}
