package com.spotify.flo.contrib.bigquery;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;

public class BigQueryClientSingleton {

  public static final BigQuery BIGQUERY_INTERNAL;
  public static final FloBigQueryClient BIGQUERY_CLIENT;

  static{
    BIGQUERY_INTERNAL = BigQueryOptions.getDefaultInstance().getService();
    BIGQUERY_CLIENT = new DefaultBigQueryClient(BIGQUERY_INTERNAL);
  }

}
