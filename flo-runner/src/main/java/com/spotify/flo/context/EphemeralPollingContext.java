package com.spotify.flo.context;

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

import com.spotify.flo.EvalContext;
import com.spotify.flo.Fn;
import com.spotify.flo.Tracing;
import com.spotify.flo.freezer.PersistingContext;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EphemeralPollingContext extends ForwardingEvalContext {

  private static final Logger log = LoggerFactory.getLogger(EphemeralPollingContext.class);

  private EphemeralPollingContext(EvalContext delegate) {
    super(delegate);
  }

  public static EvalContext composeWith(EvalContext baseContext) {
    return new EphemeralPollingContext(baseContext);
  }

  @Override
  public <T> Value<T> value(Fn<T> value) {
    return delegate.value(() -> {
      // TODO: allow flo runner to be ephemeral and shut down in between polls - use flo-freezer?
      Fn<T> currentValue = value;
      final String valueFileName = PersistingContext.cleanForFilename(Tracing.TASK_ID.get());
      try {
        while (true) {
          try {
            return currentValue.get();
          } catch (NotDoneException ignore) {
            final String valueFileNameTmp = valueFileName + ".tmp";
            try {
              PersistingContext.serialize(value, Paths.get(valueFileNameTmp));
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
            try {
              Files.move(Paths.get(valueFileNameTmp), Paths.get(valueFileName), REPLACE_EXISTING, ATOMIC_MOVE);
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
            try {
              Thread.sleep(1000);
            } catch (InterruptedException e1) {
              Thread.currentThread().interrupt();
              throw new RuntimeException("interrupted");
            }
            try {
              currentValue = PersistingContext.deserialize(Paths.get(valueFileName));
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          }
        }
      } finally {
        try {
          Files.deleteIfExists(Paths.get(valueFileName));
        } catch (IOException e) {
          log.error("Failed to delete value file: {}", valueFileName, e);
        }
      }
    });
  }
}
