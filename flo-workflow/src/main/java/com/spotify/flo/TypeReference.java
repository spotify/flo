package com.spotify.flo;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

public abstract class TypeReference<Z> {

  private final Type type;

  public TypeReference() {
    Type superClass = getClass().getGenericSuperclass();
    this.type = ((ParameterizedType) superClass).getActualTypeArguments()[0];
  }

  Class<Z> klass() {
    if (type instanceof ParameterizedType) {
      return (Class<Z>) ((ParameterizedType) type).getRawType();
    } else {
      return (Class<Z>) type;
    }
  }
}
