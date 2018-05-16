/*-
 * -\-\-
 * flo-bigquery
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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.RetryOption;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.JobStatus;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class BigQueryContextTest {

  @Mock
  private BigQuery bigQuery;

  @Mock
  private Dataset dataset;
  
  @Mock
  private Job job;

  private static final String PROJECT = "project";
  private static final String LOCATION = "EU";
  private static final DatasetId DATASET_ID = DatasetId.of(PROJECT, "dataset");
  private static final TableId TABLE_ID = TableId.of(PROJECT, DATASET_ID.getDataset(), "table");

  @Before
  public void setup() {
    when(dataset.getLocation()).thenReturn(LOCATION);
    when(bigQuery.create(any(DatasetInfo.class))).thenReturn(dataset);
    when(bigQuery.create(any(JobInfo.class))).thenReturn(mock(Job.class));
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldNotCreateDataset() {
    final BigQueryContext bigQueryContext = BigQueryContext.create(bigQuery, TABLE_ID);

    bigQueryContext.provide(null);
  }

  @Test
  public void shouldCreateStagingDatasetIfDoesNotExist() {
    when(bigQuery.getDataset(DATASET_ID)).thenReturn(dataset);

    final BigQueryContext bigQueryContext = BigQueryContext.create(bigQuery, TABLE_ID);

    bigQueryContext.provide(null);

    verify(bigQuery).create(any(DatasetInfo.class));
  }

  @Test
  public void shouldProvideStagingTableId() {
    when(bigQuery.getDataset(any(DatasetId.class))).thenReturn(dataset);

    final BigQueryContext bigQueryContext = BigQueryContext.create(bigQuery, TABLE_ID);

    final StagingTableId stagingTableId = bigQueryContext.provide(null);

    assertThat(stagingTableId.tableId(), is(not(TABLE_ID)));
  }

  @Test
  public void shouldReturnTableIdWhenCallingPublish() throws InterruptedException {
    when(bigQuery.getDataset(any(DatasetId.class))).thenReturn(dataset);
    when(bigQuery.create(any(JobInfo.class))).thenReturn(job);
    when(job.waitFor(any(RetryOption.class))).thenReturn(job);
    
    final JobStatus jobStatus = mock(JobStatus.class);
    when(job.getStatus()).thenReturn(jobStatus);

    final BigQueryContext bigQueryContext = BigQueryContext.create(bigQuery, TABLE_ID);

    final StagingTableId stagingTableId = bigQueryContext.provide(null);

    final TableId tableId = stagingTableId.publish();

    assertThat(tableId, is(TABLE_ID));
  }

  @Test
  public void shouldReturnTableIdWhenExists() {
    when(bigQuery.getDataset(DATASET_ID)).thenReturn(mock(Dataset.class));
    when(bigQuery.getTable(TABLE_ID)).thenReturn(mock(Table.class));

    final BigQueryContext bigQueryContext = BigQueryContext.create(bigQuery, TABLE_ID);

    final TableId tableId = bigQueryContext.lookup(null).get();

    assertThat(tableId, is(TABLE_ID));
  }
}
