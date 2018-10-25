/*-
 * -\-\-
 * Flo Extract
 * --
 * Copyright (C) 2016 - 2018 Spotify AB
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

package com.spotify.flo.hades;

import com.spotify.flo.Invokable;
import com.spotify.flo.OperationExtractingContext;
import com.spotify.flo.Task;
import com.spotify.flo.TaskBridge;
import com.spotify.flo.TaskOperator;
import com.spotify.flo.hades.HadesLookupSpec.LatestRevision;
import io.norberg.automatter.AutoMatter;
import java.util.Optional;

public class HadesBridge {

  public static boolean isHadesLookup(Task<?> task) {
    final Optional<? extends TaskOperator<?, ?, ?>> operator = OperationExtractingContext.operator(task);
    return operator.map(op -> isHadesLookup(op)).orElse(false);
  }

  public static boolean isHadesLookup(TaskOperator<?, ?, ?> op) {
    return op instanceof HadesLookupOperator;
  }

  public static HadesEndpointPartition endpointPartition(Task<?> task) {
    final Invokable processFn = TaskBridge.processFn(task);
    final HadesLookupSpec spec = (HadesLookupSpec) processFn.invoke(new HadesLookup());
    final HadesEndpointPartitionBuilder builder = new HadesEndpointPartitionBuilder();
    if (spec instanceof LatestRevision) {
      builder.endpoint(((LatestRevision) spec).endpoint);
      builder.partition(((LatestRevision) spec).partition);
    }
    return builder.build();
  }

  @AutoMatter
  public interface HadesEndpointPartition {

    String endpoint();

    String partition();
  }
}
