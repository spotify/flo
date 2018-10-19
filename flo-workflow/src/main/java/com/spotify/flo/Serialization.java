/*-
 * -\-\-
 * Flo Workflow Definition
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

package com.spotify.flo;

import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.nio.file.StandardOpenOption.WRITE;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamException;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Internal API.
 */
public class Serialization {

  static {
    // Best effort. Hope that ObjectOutputStream has not been loaded yet :pray:
    System.setProperty("sun.io.serialization.extendedDebugInfo", "true");
  }

  private Serialization() {
    throw new UnsupportedOperationException();
  }

  public static void serialize(Object object, Path file) throws IOException {
    serialize(object, Files.newOutputStream(file, WRITE, CREATE_NEW));
  }

  public static void serialize(Object object, OutputStream outputStream) throws IOException {
    try (ObjectOutputStream oos = new ObjectOutputStream(outputStream)) {
      oos.writeObject(object);
    }
  }

  public static byte[] serialize(Object object) throws ObjectStreamException {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try {
      serialize(object, baos);
    } catch (ObjectStreamException e) {
      throw e;
    } catch (IOException e) {
      // Should not happen
      throw new RuntimeException(e);
    }
    return baos.toByteArray();
  }

  public static <T> T deserialize(Path filePath) throws IOException, ClassNotFoundException {
    return deserialize(Files.newInputStream(filePath));
  }

  @SuppressWarnings("unchecked")
  public static <T> T deserialize(InputStream inputStream) throws IOException, ClassNotFoundException {
    try (ObjectInputStream ois = new ObjectInputStream(inputStream)) {
      return (T) ois.readObject();
    }
  }

  @SuppressWarnings("unchecked")
  public static <T> T deserialize(byte[] bytes) throws ClassNotFoundException, ObjectStreamException {
    try {
      return deserialize(new ByteArrayInputStream(bytes));
    } catch (ObjectStreamException e) {
      throw e;
    } catch (IOException e) {
      // Should not happen
      throw new RuntimeException(e);
    }
  }

  public static <T extends Serializable> T requireSerializable(T o, String name) {
    try {
      final byte[] serialized = Serialization.serialize(o);
      return Serialization.deserialize(serialized);
    } catch (ObjectStreamException | ClassNotFoundException e) {
      throw new IllegalArgumentException(name + " not serializable: " + o, e);
    }
  }
}
