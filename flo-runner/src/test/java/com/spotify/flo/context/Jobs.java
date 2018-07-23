/*-
 * -\-\-
 * Flo Runner
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

package com.spotify.flo.context;

import com.spotify.flo.EvalContext;
import com.spotify.flo.Task;
import com.spotify.flo.TaskBuilder.F0;
import com.spotify.flo.TaskBuilder.F1;
import com.spotify.flo.TaskOperator;
import java.io.Serializable;
import java.util.Map;
import java.util.function.Consumer;

class Jobs {

  static class JobSpec implements Serializable {

    private F0<Map<String, ?>> options;
    private SerializableConsumer<JobContext> pipelineConfigurator;
    private SerializableConsumer<JobResult> resultValidator;
    private F1<JobResult, ?> successHandler;

    public JobSpec options(F0<Map<String, ?>> options) {
      this.options = options;
      return this;
    }

    public JobSpec pipeline(SerializableConsumer<JobContext> pipeline) {
      this.pipelineConfigurator = pipeline;
      return this;
    }

    public JobSpec validation(SerializableConsumer<JobResult> validator) {
      this.resultValidator = validator;
      return this;
    }

    @SuppressWarnings("unchecked")
    public <T> T success(F1<JobResult, T> successHandler) {
      this.successHandler = successHandler;
      throw new TaskOperator.SpecException(this);
    }
  }

  static class JobOperator extends TaskOperator<JobSpec> {

    @Override
    public JobSpec provide(EvalContext evalContext) {
      return new JobSpec();
    }

    @Override
    public <T> T run(Task<?> task, JobSpec spec) {
      final JobContext jobContext = new JobContext(spec.options.get());
      spec.pipelineConfigurator.accept(jobContext);
      final JobResult result = jobContext.run();
      spec.resultValidator.accept(result);
      @SuppressWarnings("unchecked") final T value = (T) spec.successHandler.apply(result);
      return value;
    }

    static JobOperator create() {
      return new JobOperator();
    }
  }

  static class JobContext {

    public JobContext(Map<String, ?> options) {
    }

    public JobContext readFrom(String src) {
      return this;
    }

    public JobContext map(String operation) {
      return this;
    }


    public JobContext writeTo(String dst) {
      return this;
    }

    public JobResult run() {
      return new JobResult(4711);
    }
  }

  static class JobResult {

    final int records;

    public JobResult(int records) {
      this.records = records;
    }
  }


  interface SerializableConsumer<T> extends Consumer<T>, Serializable {

  }
}
