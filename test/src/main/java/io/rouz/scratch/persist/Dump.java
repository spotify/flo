package io.rouz.scratch.persist;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.spotify.apollo.Client;
import com.spotify.apollo.Request;
import com.spotify.apollo.Response;
import com.spotify.apollo.Status;
import io.rouz.flo.Fn;
import io.rouz.flo.Task;
import io.rouz.flo.TaskContext;
import io.rouz.flo.TaskId;
import io.rouz.flo.context.AsyncContext;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import okio.ByteString;

/**
 * TODO: document.
 */
public class Dump extends AsyncContext {

  private static final String LOCK_PATH = "/lock";

  private final ScheduledExecutorService scheduler;
  private final ExecutorService executor;
  private final ConcurrentMap<TaskId, EvalBundle<?>> ongoing = Maps.newConcurrentMap();
//  private final Logging logging;

  private final Client client = r -> CompletableFuture.completedFuture(null);

  protected Dump(
      ScheduledExecutorService scheduledExecutorService,
      ExecutorService executor) {
    super(executor);
    this.scheduler = Objects.requireNonNull(scheduledExecutorService);
    this.executor = Objects.requireNonNull(executor);
  }

  @Override
  public <T> TaskContext.Value<T> invokeProcessFn(TaskId taskId, Fn<TaskContext.Value<T>> processFn) {
    final EvalBundle<T> evalBundle = lookupBundle(taskId);
    final TaskContext.Promise<T> promise = promise();
    final LockHolder lockHolder = new LockHolder(taskId, "/", scheduler);
    final ProcessBundle<T> processBundle =
        new ProcessBundle<>(evalBundle, lockHolder, processFn, promise);

    processBundle.process();

    return promise.value();
  }

  private Runnable onExecutor(Runnable runnable) {
    return () -> executor.submit(runnable);
  }

  private <T> Consumer<T> onExecutor(Consumer<T> consumer) {
    return (t) -> executor.submit(() -> consumer.accept(t));
  }

  private <T> void chain(TaskContext.Value<T> value, TaskContext.Promise<T> promise) {
    value.consume(promise::set);
    value.onFail(promise::fail);
  }

  private static <T> Optional<Function<T, ByteString>> findEncoder(Class<T> type) {
    for (Method method : type.getDeclaredMethods()) {
      if (method.getDeclaredAnnotation(Encoder.class) != null) {
        // todo: validate signature
        return Optional.of(t -> (ByteString) invokeAndPropagateException(method, t));
      }
    }

    return Optional.empty();
  }

  private static Object invokeAndPropagateException(Method method, Object... args) {
    try {
      return method.invoke(/* static */ null, args);
    } catch (IllegalAccessException | InvocationTargetException e) {
      throw Throwables.propagate(e);
    }
  }

  private <T> EvalBundle<T> lookupBundle(TaskId taskId) {
    EvalBundle<T> spin;
    do {
      //noinspection unchecked
      spin = (EvalBundle<T>) ongoing.get(taskId);
    } while (spin == null);
    return spin;
  }

  private Request request(String method) {
    return Request.forUri("/" + LOCK_PATH, method);
  }

  static ByteString json(TaskId taskId) {
    // todo: types
    return ByteString.encodeUtf8("{\"task_id\":\"" + taskId.toString() + "\"}");
  }

  static ByteString json(TaskId taskId, ByteString data) {
    // todo: types
    return ByteString.encodeUtf8("{\"task_id\":\"" + taskId.toString() + "\", \"data\":\""
                                 + data.base64() + "\"}");
  }

  private final class EvalBundle<T> {

    private final Task<T> task;
    private final TaskContext.Promise<T> promise;
    private final Optional<Function<ByteString, T>> decoder;

    private EvalBundle(Task<T> task, TaskContext.Promise<T> promise, Optional<Function<ByteString, T>> decoder) {
      this.task = task;
      this.promise = promise;
      this.decoder = decoder;
    }

    private void fetchOrElse(TaskContext.Promise<T> promise, Runnable orElse) {
      Preconditions.checkState(decoder.isPresent(), "Must have decoder when fetching existing");

      final Request getRequest = request("GET")
          .withPayload(json(task.id()));

      client.send(getRequest)
          .handleAsync(parseExisting(decoder.get()), executor) // fixme: parse errors should fail
          .thenAcceptAsync(consumer(promise, orElse), executor);
    }

    private Consumer<TaskContext.Value<T>> consumer(TaskContext.Promise<T> promise, Runnable orElse) {
      return (lookupValue) -> {
        lookupValue.consume((value) -> {
//          logging.foundValue(task.id(), value);
          promise.set(value);
        });
        lookupValue.onFail((ˍ) -> orElse.run());
      };
    }

    private BiFunction<Response<ByteString>, Throwable, TaskContext.Value<T>> parseExisting(
        Function<ByteString, T> decoder) {
      return (response, throwable) -> {
        final TaskContext.Promise<T> promise = promise();

        if (throwable != null) {
          promise.fail(throwable);
        } else if (response.status().code() != Status.OK.code()) {
          promise.fail(new RuntimeException("Not OK"));
        } else if (!response.payload().isPresent()) {
          promise.fail(new RuntimeException("Response with no body returned for persisted value from flock"));
        } else {
          final ByteString data = response.payload().get();
          final T value = decoder.apply(data);
          promise.set(value);
        }

        return promise.value();
      };
    }
  }

  private final class ProcessBundle<T> {

    private final EvalBundle<T> evalBundle;
    private final LockHolder lockHolder;
    private final Fn<TaskContext.Value<T>> processFn;
    private final TaskContext.Promise<T> promise;

    private final Semaphore notified = new Semaphore(1);

    public ProcessBundle(
        EvalBundle<T> evalBundle,
        LockHolder lockHolder,
        Fn<TaskContext.Value<T>> processFn,
        TaskContext.Promise<T> promise) {
      this.evalBundle = evalBundle;
      this.lockHolder = lockHolder;
      this.processFn = processFn;
      this.promise = promise;
    }

    private void process() {
      Consumer<Throwable> whenLocked = onExecutor((throwable) -> {
        final TaskId taskId = evalBundle.task.id();

        if (throwable != null) {
          if (throwable instanceof LockHolder.AlreadyLocked) {
            if (notified.tryAcquire()) {
//              logging.waiting(taskId);
            }

            evalBundle.fetchOrElse(promise, () -> scheduler.schedule(onExecutor(() -> {
//              logging.polling(taskId);
              process();
            }), 1, TimeUnit.SECONDS)); // todo: already locked: wait without polling
          } else {
//            logging.lockFailed(taskId, throwable);
            promise.fail(throwable);
          }
          return;
        }

        invoke();
      });

      if (shouldLock()) {
        lockHolder.lock(whenLocked);
      } else {
        invoke();
      }
    }

    private void invoke() {
      TaskId taskId = evalBundle.task.id();
      final TaskContext.Value<T> value = processFn.get();

      value.consume((v) -> {
        complete(
            v,
            () -> {
//              logging.completedValue(taskId, v);
              promise.set(v);
            }, (t) -> {
//              logging.failedValue(taskId, t);
              promise.fail(t);
            });
      });
      value.onFail((valueError) -> {
//        logging.failedValue(taskId, valueError);
        fail(() -> promise.fail(valueError));
      });
    }

    private void complete(T value, Runnable callback, Consumer<Throwable> failure) {
      if (shouldLock()) {
        final Optional<ByteString> encoded;
        try {
          encoded = findEncoder(evalBundle.task.type()).map(fn -> fn.apply(value));
        } catch (Throwable t) {
          lockHolder.unlock(() -> failure.accept(t));
          return;
        }

        if (encoded.isPresent()) {
          lockHolder.unlock(encoded.get(), onExecutor(callback));
        } else {
          lockHolder.unlock(onExecutor(callback));
        }
      } else {
        callback.run();
      }
    }

    private void fail(Runnable callback) {
      if (shouldLock()) {
        lockHolder.unlock(onExecutor(callback));
      } else {
        callback.run();
      }
    }

    private boolean shouldLock() {
      return evalBundle.decoder.isPresent();
    }
  }
}
