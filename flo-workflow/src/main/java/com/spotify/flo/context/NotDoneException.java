package com.spotify.flo.context;

public class NotDoneException extends RuntimeException {

  private NotDoneException() {
    if (NOT_DONE != null) {
      throw new UnsupportedOperationException();
    }
  }

  public static final NotDoneException NOT_DONE = new NotDoneException();
}
