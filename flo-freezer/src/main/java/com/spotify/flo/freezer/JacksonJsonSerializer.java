/*-
 * -\-\-
 * Flo Freezer
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

package com.spotify.flo.freezer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Registration;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.io.IOException;
import java.util.Optional;

/**
 * A {@link Kryo} {@link Serializer} that serializes types marked with {@link JsonSerialize} as json using Jackson.
 */
class JacksonJsonSerializer extends Serializer {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  static Optional<Registration> getRegistration(Kryo kryo, Class type) {
    return hasJsonSerializeAnnotation(type)
        ? Optional.of(kryo.getRegistration(RegistrationMarker.class))
        : Optional.empty();
  }

  static void register(Kryo kryo) {
    kryo.register(RegistrationMarker.class, new JacksonJsonSerializer());
  }

  @Override
  public void write(Kryo kryo, Output output, Object object) {
    kryo.writeObject(output, object.getClass(), new JavaSerializer());
    final byte[] json;
    try {
      json = MAPPER.writeValueAsBytes(object);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
    kryo.writeObject(output, json);
  }

  @Override
  public Object read(Kryo kryo, Input input, Class type) {
    final Class<?> klass = kryo.readObject(input, Class.class, new JavaSerializer());
    final byte[] json = kryo.readObject(input, byte[].class);
    try {
      return MAPPER.readValue(json, klass);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static boolean hasJsonSerializeAnnotation(Class type) {
    if (type.getAnnotation(JsonSerialize.class) != null) {
      return true;
    }
    final Class superclass = type.getSuperclass();
    if (superclass != null && hasJsonSerializeAnnotation(superclass)) {
      return true;
    }
    for (Class iface : type.getInterfaces()) {
      if (hasJsonSerializeAnnotation(iface)) {
        return true;
      }
    }
    return false;
  }

  private static final class RegistrationMarker {

    private RegistrationMarker() {
      throw new UnsupportedOperationException();
    }
  }
}
