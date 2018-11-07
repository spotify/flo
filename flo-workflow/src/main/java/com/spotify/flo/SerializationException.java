package com.spotify.flo;

/**
 * Thrown by {@link Serialization#serialize} and {@link Serialization#deserialize}
 * if serialization or deserialization fails.
 */
public class SerializationException extends Exception {

  public SerializationException(String message, Throwable cause) {
    super(message, cause);
  }
}
