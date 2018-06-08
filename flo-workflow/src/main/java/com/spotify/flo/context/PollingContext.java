package com.spotify.flo.context;

import com.spotify.flo.EvalContext;
import com.spotify.flo.Fn;

public class PollingContext extends ForwardingEvalContext {

  private PollingContext(EvalContext delegate) {
    super(delegate);
  }

  public static EvalContext composeWith(EvalContext baseContext) {
    return new PollingContext(baseContext);
  }

  @Override
  public <T> Value<T> value(Fn<T> value) {
    return delegate.value(() -> {
      // TODO: serialize value between polls etc
      while (true) {
        try {
          return value.get();
        } catch (NotDoneException e) {
          try {
            Thread.sleep(1000);
          } catch (InterruptedException e1) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("interrupted");
          }
        }
      }
    });
  }
}
