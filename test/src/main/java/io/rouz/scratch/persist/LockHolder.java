package io.rouz.scratch.persist;

import com.spotify.apollo.Client;
import com.spotify.apollo.Request;
import com.spotify.apollo.Status;
import io.rouz.flo.TaskId;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import okio.ByteString;

/**
 * TODO: document.
 */
class LockHolder {

  private static final int RENEW_INTERVAL = 10;

  private final TaskId id;
  private final String request;
  private final ScheduledExecutorService scheduler;

  private final Client client = r -> CompletableFuture.completedFuture(null);

  private volatile boolean done = false;
  private volatile ScheduledFuture<?> schedule;

  LockHolder(
      TaskId id,
      String request,
      ScheduledExecutorService scheduler) {
    this.id = Objects.requireNonNull(id);
    this.request = Objects.requireNonNull(request);
    this.scheduler = Objects.requireNonNull(scheduler);
  }

  void lock(Consumer<Throwable> whenLocked) {
    final Request lockRequest = Request.forUri(request, "POST")
        .withPayload(Dump.json(id));

    client.send(lockRequest)
        .whenComplete((response, throwable) -> {

          // todo: fix error handling (exception or already locked)

          if (throwable != null) {
            whenLocked.accept(throwable);
            return;
          }

          if (response.status().code() == Status.CONFLICT.code()) {
            whenLocked.accept(new AlreadyLocked());
            return;
          }

          if (response.status().code() != Status.OK.code()) {
            whenLocked.accept(new IllegalStateException(String.format(
                "Could not lock %s - %s %s",
                id, response.status().code(), response.status().reasonPhrase())));
            return;
          }

//          logging.locked(id, response.payload().get().utf8());
          schedule();

          // successfully locked, null exception
          whenLocked.accept(null);
        });
  }

  void unlock(Runnable whenUnlocked) {
    final Request deleteRequest = Request.forUri(request, "DELETE")
        .withPayload(Dump.json(id));

    unlock(deleteRequest, whenUnlocked);
  }

  void unlock(ByteString data, Runnable whenUnlocked) {
    final Request deleteRequest = Request.forUri(request, "DELETE")
        .withPayload(Dump.json(id, data));

    unlock(deleteRequest, whenUnlocked);
  }

  private void unlock(Request deleteRequest, Runnable whenUnlocked) {
//    logging.unlock(id);
    done = true;
    if (schedule != null) {
      schedule.cancel(false);
    }

    client.send(deleteRequest) // todo: validate unlock
        .thenAccept((reply) -> whenUnlocked.run());
  }

  private void schedule() {
    schedule = scheduler.schedule(this::renew, RENEW_INTERVAL, TimeUnit.SECONDS);
  }

  private void renew() {
    if (done) {
      return;
    }

    final Request renewRequest = Request.forUri(request, "PUT")
        .withPayload(Dump.json(id));

//    logging.renewLock(id);
    client.send(renewRequest)
        .whenComplete((response, throwable) -> {
          if (throwable != null) {
            throwable.printStackTrace();
            return;
          }

          if (response.status().code() != Status.OK.code()) {
//            logging.renewFailed(id, response.status());
          } else {
//            logging.renewSuccess(id, response.payload().get().utf8());
          }

          schedule();
        });
  }

  static class AlreadyLocked extends Exception {
  }
}
