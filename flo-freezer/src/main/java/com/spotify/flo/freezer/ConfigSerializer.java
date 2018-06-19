/*-
 * -\-\-
 * flo-freezer
 * --
 * Copyright (C) 2016 - 2017 Spotify AB
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
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.MapSerializer;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.util.HashMap;
import java.util.Map;

public class ConfigSerializer extends Serializer<Config> {

  @Override
  public void write(Kryo kryo, Output output, Config object) {
    MapSerializer serializer = new MapSerializer();
    kryo.writeObject(output, object.root().unwrapped(), serializer);
  }

  @Override
  public Config read(Kryo kryo, Input input, Class<Config> type) {
    MapSerializer serializer = new MapSerializer();
    Map configMap = kryo.readObject(input, HashMap.class, serializer);
    return ConfigFactory.parseMap(configMap);
  }

}
