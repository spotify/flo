/*-
 * -\-\-
 * flo runner
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

package com.spotify.flo.context;

import com.typesafe.config.Config;

/**
 * A Factory for creating a {@link InstrumentedContext.Listener}. Factory implementations will be
 * loaded by {@link FloRunner} through the {@link java.util.ServiceLoader#load(Class)} mechanism.
 *
 * The {@link #createListener(Config)} method gets a reference to the loaded {@link Config}
 * object which can be used to extract information about the runner configuration.
 *
 * An example is to use {@link StyxExecutionId#fromConfig(Config)} to load information about a
 * Styx execution context, in the case the workflow is run by Styx.
 */
public interface FloListenerFactory {

  InstrumentedContext.Listener createListener(Config config);
}
