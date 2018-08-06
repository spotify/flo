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

import com.google.common.collect.ImmutableMap;
import com.spotify.flo.EvalContext;
import com.spotify.flo.TaskBuilder.F0;
import com.spotify.flo.TaskBuilder.F1;
import com.spotify.flo.TaskId;
import com.spotify.flo.TaskOperator;
import com.spotify.flo.TaskOperator.Listener;
import com.spotify.flo.TaskOperator.Operation;
import java.io.Serializable;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class Jobs {

  private static final Logger log = LoggerFactory.getLogger(Jobs.class);

  static class JobSpec<T> implements Serializable {

    private static final long serialVersionUID = 1L;

    private final TaskId taskId;

    private F0<Map<String, ?>> options = Collections::emptyMap;
    private SerializableConsumer<JobContext> pipelineConfigurator = ctx -> {};
    private SerializableConsumer<JobResult> resultValidator = result -> {};
    private F1<JobResult, T> successHandler;

    JobSpec(TaskId taskId) {
      this.taskId = taskId;
    }

    public JobSpec<T> options(F0<Map<String, ?>> options) {
      this.options = options;
      return this;
    }

    public JobSpec<T> pipeline(SerializableConsumer<JobContext> pipeline) {
      this.pipelineConfigurator = pipeline;
      return this;
    }

    public JobSpec<T> validation(SerializableConsumer<JobResult> validator) {
      this.resultValidator = validator;
      return this;
    }

    public JobSpec<T> success(F1<JobResult, T> successHandler) {
      this.successHandler = successHandler;
      return this;
    }
  }

  static class JobOperator<T> implements TaskOperator<JobSpec<T>, JobSpec<T>, T> {

    @Override
    public JobSpec<T> provide(EvalContext evalContext) {
      return new JobSpec<>(evalContext.currentTask().get().id());
    }

    static <T> JobOperator<T> create() {
      return new JobOperator<>();
    }

    @Override
    public JobOperation<T> start(JobSpec<T> spec, Listener listener) {
      final JobContext jobContext = new JobContext(spec.options.get());
      spec.pipelineConfigurator.accept(jobContext);
      final Job job = jobContext.run();
      listener.meta(spec.taskId, ImmutableMap.of(
          "task-id", spec.taskId.toString(),
          "job-id", job.id));
      log.info("started job: {}", job.id);
      return new JobOperation<>(spec, job);
    }

  }

  private static class JobOperation<T> implements Operation<T, JobOperationState> {

    private static final long serialVersionUID = 1L;

    private final JobSpec<T> spec;
    private final Job job;

    public JobOperation(JobSpec<T> spec, Job job) {
      this.spec = spec;
      this.job = job;
    }

    @Override
    public Result<T, JobOperationState> perform(Optional<JobOperationState> prevState, Listener listener) {
      JobOperationState nextState = prevState.map(JobOperationState::increment)
          .orElseGet(JobOperationState::new);

      log.info("checking job completion: {}", job.id);

      // Not yet done?
      if (nextState.invocations < 3) {
        log.info("job {} not yet completed", job.id);
        return Result.ofContinuation(Duration.ofSeconds((long) Math.pow(2, nextState.invocations)), nextState);
      }

      // Done!
      final JobResult result = new JobResult(4711);

      // Validate
      log.info("validating job {} result", job.id);
      try {
        spec.resultValidator.accept(result);
      } catch (Throwable t) {
        return Result.ofFailure(t);
      }

      // Return output
      log.info("job {} successfully completed", job.id);
      final T output = spec.successHandler.apply(result);
      return Result.ofSuccess(output);
    }

    @Override
    public String toString() {
      return "JobOperation{" + job.id + '}';
    }
  }

  private static class JobOperationState implements Serializable {

    private static final long serialVersionUID = 1L;

    final int invocations;

    JobOperationState() {
      this(1);
    }

    JobOperationState(int invocations) {
      this.invocations = invocations;
    }

    JobOperationState increment() {
      return new JobOperationState(invocations + 1);
    }

    @Override
    public String toString() {
      return "JobOperationState{" +
          "invocations=" + invocations +
          '}';
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

    public Job run() {
      return new Job();
    }
  }

  static class Job implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String id;

    public Job() {
      this.id = UUID.randomUUID().toString();
    }

    @Override
    public String toString() {
      return "Job{" +
          "id='" + id + '\'' +
          '}';
    }
  }

  static class JobResult implements Serializable {

    private static final long serialVersionUID = 1L;

    final int records;

    public JobResult(int records) {
      this.records = records;
    }
  }
}
