/*
 * -\-\-
 * Copyright (C) 2016 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 * -/-/-
 */

package com.spotify.flo.context;

import static com.google.cloud.storage.contrib.nio.CloudStorageOptions.withMimeType;
import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.nio.file.StandardOpenOption.WRITE;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.stream.Collectors.toList;

import com.fasterxml.jackson.core.Base64Variants;
import com.google.api.client.util.BackOff;
import com.google.api.client.util.Sleeper;
import com.google.auto.value.AutoValue;
import com.google.cloud.hadoop.util.ApiErrorExtractor;
import com.google.common.hash.Funnels;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.common.io.CountingOutputStream;
import com.google.common.io.Files;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.file.NoSuchFileException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper for staging classpath elements to GCS
 *
 * NOTICE: Copied from https://github.com/GoogleCloudPlatform/DataflowJavaSDK
 *         Modified to use gcloud-java-nio
 *         https://github.com/GoogleCloudPlatform/google-cloud-java/tree/gcs-nio/gcloud-java-contrib/gcloud-java-nio
 */
public class StagingUtil {

  private static final Logger LOG = LoggerFactory.getLogger(StagingUtil.class);

  private static final String BINARY = "application/octet-stream";

  /**
   * A reasonable upper bound on the number of jars required to launch a Dataflow job.
   */
  private static final int SANE_CLASSPATH_SIZE = 1000;
  /**
   * The initial interval to use between package staging attempts.
   */
  private static final Duration INITIAL_BACKOFF_INTERVAL = Duration.ofSeconds(5);
  /**
   * The maximum number of retries when staging a file.
   */
  private static final int MAX_RETRIES = 4;

  private static final FluentBackoff BACKOFF_FACTORY =
      FluentBackoff.DEFAULT
          .withMaxRetries(MAX_RETRIES).withInitialBackoff(INITIAL_BACKOFF_INTERVAL);

  /**
   * Translates exceptions from API calls.
   */
  private static final ApiErrorExtractor ERROR_EXTRACTOR = new ApiErrorExtractor();

  /**
   * Static cache for uploaded files. Used to avoid reading and checking the status of a file
   * repeatedly within the same process.
   */
  private static final ConcurrentMap<UploadPair, ListenableFuture<StagedPackage>> UPLOAD_CACHE =
      new ConcurrentHashMap<>();

  private static final AtomicInteger UPLOAD_CALL_COUNTER = new AtomicInteger(0);
  private static final ForkJoinPool FJP = new ForkJoinPool(32);
  private static final long UPLOAD_TIMEOUT_MINUTES = 10;

  /**
   * Transfers the classpath elements to the staging location.
   *
   * @param classpathElements The elements to stage.
   * @param stagingPath The base location to stage the elements to.
   * @return A list of cloud workflow packages, each representing a classpath element.
   */
  public static List<StagedPackage> stageClasspathElements(
      Collection<String> classpathElements, String stagingPath) {
    LOG.info("Uploading {} files to staging location {} to "
             + "prepare for execution.", classpathElements.size(), stagingPath);

    if (classpathElements.size() > SANE_CLASSPATH_SIZE) {
      LOG.warn("Your classpath contains {} elements, which Google Cloud Dataflow automatically "
               + "copies to all workers. Having this many entries on your classpath may be indicative "
               + "of an issue in your pipeline. You may want to consider trimming the classpath to "
               + "necessary dependencies only, using --filesToStage pipeline option to override "
               + "what files are being staged, or bundling several dependencies into one.",
          classpathElements.size());
    }

    if (stagingPath == null) {
      throw new IllegalArgumentException(
          "Can't stage classpath elements on because no staging location has been provided");
    }

    final StageCallResults stageCallResults =
        new StagingCall(classpathElements, stagingPath).doStage();

    LOG.info("Uploading complete: {} files newly uploaded, {} files cached",
        stageCallResults.numUploaded(), stageCallResults.numCached());

    return stageCallResults.stagedPackages();
  }

  private static void upload(String classpathElement, String target, Path targetPath)
      throws IOException, InterruptedException {
    ArrayList<OpenOption> options = new ArrayList<>();
    options.add(WRITE);
    options.add(CREATE_NEW);
    if ("gs".equals(targetPath.toUri().getScheme())) {
      options.add(withMimeType(BINARY));
    }

    // Upload file, retrying on failure.
    Sleeper retrySleeper = Sleeper.DEFAULT;
    BackOff backoff = BACKOFF_FACTORY.backoff();
    while (true) {
      try {
        LOG.debug("Uploading classpath element {} to {}", classpathElement, target);
        try (WritableByteChannel writer = java.nio.file.Files.newByteChannel(targetPath,
            options.toArray(new OpenOption[options.size()]))) {
          copyContent(classpathElement, writer);
        }
        break;
      } catch (IOException e) {
        if (ERROR_EXTRACTOR.accessDenied(e)) {
          String errorMessage = String.format(
              "Uploaded failed due to permissions error, will NOT retry staging "
              + "of classpath %s. Please verify credentials are valid and that you have "
              + "write access to %s. Stale credentials can be resolved by executing "
              + "'gcloud auth login'.", classpathElement, target);
          LOG.error(errorMessage);
          throw new IOException(errorMessage, e);
        }
        long sleep = backoff.nextBackOffMillis();
        if (sleep == BackOff.STOP) {
          // Rethrow last error, to be included as a cause in the catch below.
          LOG.error("Upload failed, will NOT retry staging of classpath: {}",
              classpathElement, e);
          throw e;
        } else {
          LOG.warn("Upload attempt failed, sleeping before retrying staging of classpath: {}",
              classpathElement, e);
          retrySleeper.sleep(sleep);
        }
      }
    }
  }

  /**
   * Returns a unique name for a file with a given content hash.
   *
   * <p>Directory paths are removed. Example:
   * <pre>
   * dir="a/b/c/d", contentHash="f000" => d-f000.jar
   * file="a/b/c/d.txt", contentHash="f000" => d-f000.txt
   * file="a/b/c/d", contentHash="f000" => d-f000
   * </pre>
   */
  private static String getUniqueContentName(File classpathElement, String contentHash) {
    String fileName = Files.getNameWithoutExtension(classpathElement.getAbsolutePath());
    String fileExtension = Files.getFileExtension(classpathElement.getAbsolutePath());
    if (classpathElement.isDirectory()) {
      return fileName + "-" + contentHash + ".jar";
    } else if (fileExtension.isEmpty()) {
      return fileName + "-" + contentHash;
    }
    return fileName + "-" + contentHash + "." + fileExtension;
  }

  /**
   * Copies the contents of the classpathElement to the output channel.
   *
   * <p>If the classpathElement is a directory, a Zip stream is constructed on the fly,
   * otherwise the file contents are copied as-is.
   *
   * <p>The output channel is not closed.
   */
  private static void copyContent(String classpathElement, WritableByteChannel outputChannel)
      throws IOException {
    final File classpathElementFile = new File(classpathElement);
    if (classpathElementFile.isDirectory()) {
      ZipFiles.zipDirectory(classpathElementFile, Channels.newOutputStream(outputChannel));
    } else {
      Files.asByteSource(classpathElementFile).copyTo(Channels.newOutputStream(outputChannel));
    }
  }

  /**
   * Method object for doing the actual staging. Since the actual upload happens in the cache, each
   * instance will have a unique id that is set on the {@link StagedPackage} objects when it is
   * uploaded. This is then used to tally the uploaded/cached counts at the end of the upload.
   */
  private static class StagingCall {

    final int id = UPLOAD_CALL_COUNTER.getAndIncrement();
    final Collection<String> classpathElements;
    final String stagingPath;

    private StagingCall(Collection<String> classpathElements, String stagingPath) {
      this.classpathElements = classpathElements;
      this.stagingPath = stagingPath;
    }

    StageCallResults doStage() {
      List<ListenableFuture<StagedPackage>> uploadFutures = classpathElements.stream()
          .filter(classpathElement -> {
            File file = new File(classpathElement);
            if (!file.exists()) {
              LOG.warn("Skipping non-existent classpath element {} that was specified.",
                  classpathElement);
              return false;
            } else {
              return true;
            }
          })
          .map(this::uploadClasspathElement)
          .collect(toList());

      List<StagedPackage> stagedPackages;
      try {
        stagedPackages = Futures.getChecked(
            Futures.allAsList(uploadFutures),
            IOException.class,
            UPLOAD_TIMEOUT_MINUTES, MINUTES);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

      // How many of these were uploaded by us vs. other calls
      int numUploaded = (int) stagedPackages.stream().filter(p -> p.stageCallId() == id).count();
      int numCached = (int) stagedPackages.stream().filter(p -> p.stageCallId() != id).count();

      return stageCallResults(stagedPackages, numUploaded, numCached);
    }

    private ListenableFuture<StagedPackage> uploadClasspathElement(String classpathElement) {
      return UPLOAD_CACHE.computeIfAbsent(uploadPair(classpathElement, stagingPath),
          this::uploadClasspathElement);
    }

    private ListenableFuture<StagedPackage> uploadClasspathElement(UploadPair uploadPair) {
      StagedPackage stagedPackage = createStagedPackage(uploadPair);
      String classpathElement = uploadPair.classpathElement();
      String target = stagedPackage.location();
      Path targetPath = Paths.get(URI.create(target));

      SettableFuture<StagedPackage> future = SettableFuture.create();

      FJP.submit(() -> {
        // TODO: Should we attempt to detect the Mime type rather than
        // always using MimeTypes.BINARY?
        try {
          try {
            long remoteLength = java.nio.file.Files.size(targetPath);
            if (remoteLength == stagedPackage.size()) {
              LOG.debug("Skipping classpath element already staged: {} at {}",
                  classpathElement, target);

              future.set(stagedPackage.asCached());
              return;
            }
          } catch (FileNotFoundException | NoSuchFileException expected) {
            // If the file doesn't exist, it means we need to upload it.
          }

          upload(classpathElement, target, targetPath);
          future.set(stagedPackage);
        } catch (Exception e) {
          future.setException(new RuntimeException("Could not stage classpath element: "
                                                   + classpathElement, e));
        }
      });

      return future;
    }

    /**
     * Compute and cache the attributes of a classpath element that we will need to stage it.
     */
    private StagedPackage createStagedPackage(UploadPair uploadPair) {
      String classpathElement = uploadPair.classpathElement();
      String stagingPath = uploadPair.stagingPath();
      String overridePackageName = null;
      if (classpathElement.contains("=")) {
        String[] components = classpathElement.split("=", 2);
        overridePackageName = components[0];
        classpathElement = components[1];
      }

      File classpathFile = new File(classpathElement);

      try {
        Path targetPathPath = Paths.get(URI.create(stagingPath));
        boolean directory = classpathFile.isDirectory();

        // Compute size and hash in one pass over file or directory.
        Hasher hasher = Hashing.md5().newHasher();
        OutputStream hashStream = Funnels.asOutputStream(hasher);
        CountingOutputStream countingOutputStream = new CountingOutputStream(hashStream);

        if (!directory) {
          // Files are staged as-is.
          Files.asByteSource(classpathFile).copyTo(countingOutputStream);
        } else {
          // Directories are recursively zipped.
          ZipFiles.zipDirectory(classpathFile, countingOutputStream);
        }

        long size = countingOutputStream.getCount();
        String hash = Base64Variants.MODIFIED_FOR_URL.encode(hasher.hash().asBytes());

        // Create the DataflowPackage with staging name and location.
        String uniqueName = getUniqueContentName(classpathFile, hash);
        Path resourcePath = targetPathPath.resolve(uniqueName);
        return stagedPackage(
            uploadPair.classpathElement(),
            overridePackageName != null ? overridePackageName : uniqueName,
            resourcePath.toUri().toString(),
            size, id);
      } catch (IOException e) {
        throw new RuntimeException("Package setup failure for " + classpathElement, e);
      }
    }
  }

  @AutoValue
  public static abstract class StagedPackage {
    public abstract String sourceName();
    public abstract String name();
    public abstract String location();
    public abstract long size();

    abstract int stageCallId();

    StagedPackage asCached() {
      return stagedPackage(sourceName(), name(), location(), size(), -1);
    }
  }

  @AutoValue
  static abstract class UploadPair {
    abstract String classpathElement();
    abstract String stagingPath();
  }

  @AutoValue
  static abstract class StageCallResults {
    abstract List<StagedPackage> stagedPackages();
    abstract int numUploaded();
    abstract int numCached();
  }

  private static StagedPackage stagedPackage(String sourceName, String name, String location, long size, int stageCallId) {
    return new AutoValue_StagingUtil_StagedPackage(sourceName, name, location, size, stageCallId);
  }

  private static UploadPair uploadPair(String classpathElement, String stagingPath) {
    return new AutoValue_StagingUtil_UploadPair(classpathElement, stagingPath);
  }

  private static StageCallResults stageCallResults(List<StagedPackage> stagedPackages,
                                                   int numUploaded, int numCached) {
    return new AutoValue_StagingUtil_StageCallResults(stagedPackages, numUploaded, numCached);
  }
}
