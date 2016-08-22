package io.rouz.flo;

/**
 * An identifier that uniquely identifies a Task Instance
 */
public interface TaskId {

  /**
   * @return The name of the task
   */
  String name();

  /**
   * @return A hash for the task parameter values
   */
  int hash();

  /**
   * Creates a {@link TaskId}.
   *
   * The task name can not contain any parenthesis.
   *
   * @throws IllegalArgumentException if the name contains any parenthesis
   * @param name  Task name
   * @param args  Arbitrary task arguments
   * @return A task id for the given arguments
   */
  static TaskId create(String name, Object... args) {
    return TaskIds.create(name, args);
  }

  /**
   * Parses a {@link TaskId} based on a string representation.
   *
   * A parsed id is equal to a task id that was created using {@link #create(String, Object...)}.
   *
   * @throws IllegalArgumentException if the string does not conform to a task id
   * @param stringId  The id to parse
   * @return A parsed task id
   */
  static TaskId parse(String stringId) {
    return TaskIds.parse(stringId);
  }
}
