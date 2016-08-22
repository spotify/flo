package io.rouz.flo;

/**
 * Used to create a {@link Task} by parsing arguments from a String array.
 */
public interface TaskConstructor<T> {

  /**
   * @return The name of the task being created
   */
  String name();

  /**
   * Create an instance of the task by parsing the arguments from a String array
   *
   * @param args  The arguments to parse
   * @return an instance of the task
   */
  Task<T> create(String... args);
}
