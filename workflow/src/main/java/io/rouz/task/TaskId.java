package io.rouz.task;

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
}
