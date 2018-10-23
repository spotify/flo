/*-
 * -\-\-
 * Flo BigQuery
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

package com.spotify.flo.contrib.bigquery;

import com.google.cloud.bigquery.TableId;
import com.spotify.flo.EvalContext;
import com.spotify.flo.TaskBuilder.F0;
import com.spotify.flo.TaskOperator;
import com.spotify.flo.contrib.bigquery.BigQueryLookup.Spec;
import com.spotify.flo.status.NotReady;
import java.util.Objects;

class BigQueryLookupOperator implements TaskOperator<BigQueryLookup, BigQueryLookup.Spec, TableId> {

  private static final long serialVersionUID = 1L;

  private final F0<FloBigQueryClient> bq;

  private BigQueryLookupOperator(F0<FloBigQueryClient> bq) {
    this.bq = Objects.requireNonNull(bq, "bq");
  }

  public static BigQueryLookupOperator of(F0<FloBigQueryClient> bq) {
    return new BigQueryLookupOperator(bq);
  }

  @Override
  public BigQueryLookup provide(EvalContext evalContext) {
    return new BigQueryLookup();
  }

  @Override
  public TableId perform(Spec spec, Listener listener) {
    return spec.lookup(bq.get()).orElseThrow(NotReady::new);
  }
}
