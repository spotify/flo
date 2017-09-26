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

package com.spotify.flo.gcs;

import static com.google.cloud.storage.Storage.BlobListOption.prefix;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.base.Preconditions;
import com.spotify.flo.Task;
import com.spotify.flo.status.NotReady;
import java.net.URI;

/**
 * A set of tasks for Google Cloud Storage.
 */
public class GcsTasks {

  private static final Storage STORAGE = StorageOptions.getDefaultInstance().getService();

  /**
   * Check if a Cloud Storage Blob exists.
   *
   * <p>This task will only succeed if there exists a blob exactly matching the uri argument.
   *
   * @param uri A uri pointing to the blob, e.g. gs://my-bucket/the/blob
   * @return A task that will complete if the blob exists
   */
  public static Task<Blob> blobExists(String uri) {
    return blobExists(URI.create(uri));
  }

  /**
   * See {@link #blobExists(String)}.
   */
  public static Task<Blob> blobExists(URI uri) {
    Preconditions.checkArgument("gs".equals(uri.getScheme()), "Uri must have 'gs' as scheme");

    return Task.named("GcsBlobExists", uri).ofType(Blob.class)
        .process(() -> blobExistsInner(uri));
  }

  private static Blob blobExistsInner(URI uri) {
    final String bucketName = uri.getAuthority();
    final String prefix = uri.getPath().startsWith("/") ? uri.getPath().substring(1) : uri.getPath();

    final Page<Blob> list = STORAGE.list(bucketName, prefix(prefix));
    for (final Blob blob : list.iterateAll()) {
      if (blob.getName().equals(prefix)) {
        return blob;
      }
    }

    throw new NotReady();
  }
}
