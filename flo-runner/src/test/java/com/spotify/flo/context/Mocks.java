package com.spotify.flo.context;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.spotify.flo.EvalContext;
import com.spotify.flo.FloTesting;
import com.spotify.flo.Task;
import com.spotify.flo.TaskContextStrict;
import com.spotify.flo.TestContext;
import com.spotify.flo.status.NotReady;
import java.net.URI;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

@SuppressWarnings("WeakerAccess")
public class Mocks {

  public static class DataProcessing {

    private static final TestContext.Key<DataProcessing.Mocking> MOCK =
        TestContext.key("data-processing-mock", DataProcessing.Mocking::new);

    public static String runJob(final String job, final URI data) {
      if (FloTesting.isTest()) {
        // Return mock data
        return mock().doRunJob(job, data);
      } else {
        // Talk to some production system and run a MR job etc
        throw new UnsupportedOperationException();
      }
    }

    public static Mocking mock() {
      return MOCK.get();
    }

    public static class Mocking {

      private final ConcurrentMap<String, ConcurrentMap<URI, String>> results = new ConcurrentHashMap<>();
      private final ConcurrentMap<String, ConcurrentMap<URI, AtomicInteger>> runs = new ConcurrentHashMap<>();

      public void result(String job, URI data, String result) {
        results.computeIfAbsent(job, j -> new ConcurrentHashMap<>()).put(data, result);
      }

      String doRunJob(String job, URI data) {
        runs.computeIfAbsent(job, j -> new ConcurrentHashMap<>())
            .computeIfAbsent(data, d -> new AtomicInteger())
            .incrementAndGet();
        return Optional.ofNullable(results.get(job))
            .flatMap(jobResults -> Optional.ofNullable(jobResults.get(data)))
            .orElseThrow(() -> new AssertionError("mock results not found for job " + job + " with data " + data));
      }

      int jobRuns(String job, URI data) {
        return Optional.ofNullable(runs.get(job))
            .flatMap(r -> Optional.ofNullable(r.get(data)))
            .map(AtomicInteger::get).orElse(0);
      }
    }
  }

  public static class StorageLookup {

    private static final TestContext.Key<StorageLookup.Mocking> MOCK =
        TestContext.key("storage-lookup-mock", Mocking::new);

    public static Task<URI> of(String key) {
      return Task.named("lookup", key).ofType(URI.class)
          .process(() -> {
            if (FloTesting.isTest()) {
              return mock().lookup(key).orElseThrow(NotReady::new);
            } else {
              // Talk to some production storage system like HDFS etc
              throw new UnsupportedOperationException();
            }
          });
    }

    public static Mocking mock() {
      return MOCK.get();
    }

    public static class Mocking {

      private final ConcurrentMap<String, URI> lookupValues = new ConcurrentHashMap<>();
      private final ConcurrentMap<String, AtomicInteger> lookupOperations = new ConcurrentHashMap<>();

      private Optional<URI> lookup(String key) {
        lookupOperations.computeIfAbsent(key, k -> new AtomicInteger()).incrementAndGet();
        return Optional.ofNullable(lookupValues.get(key));
      }

      public void data(String key, URI data) {
        lookupValues.put(key, data);
      }

      public int lookups(String bar) {
        return Optional.ofNullable(lookupOperations.get(bar)).map(AtomicInteger::get).orElse(0);
      }
    }
  }

  public static class PublishingContext extends TaskContextStrict<PublishingContext.Value, String> {

    private static final TestContext.Key<Mocking> MOCK = TestContext.key("publishing-context-mock", Mocking::new);

    private final String key;

    private PublishingContext(String key) {
      this.key = key;
    }

    public static PublishingContext of(String key) {
      return new PublishingContext(key);
    }

    @Override
    public Value provide(EvalContext evalContext) {
      return new Value();
    }

    @Override
    public Optional<String> lookup(Task task) {
      if (FloTesting.isTest()) {
        return MOCK.get().doLookup(key);
      } else {
        // Talk to some production service
        throw new UnsupportedOperationException();
      }
    }

    public static Mocking mock() {
      return MOCK.get();
    }

    public class Value {

      public String publish(String value) {
        if (FloTesting.isTest()) {
          return MOCK.get().doPublish(key, value);
        } else {
          // Talk to some production service
          throw new UnsupportedOperationException();
        }
      }
    }

    public static class Mocking {

      private final ConcurrentMap<String, String> lookupValues = new ConcurrentHashMap<>();
      private final ConcurrentMap<String, AtomicInteger> lookupOperations = new ConcurrentHashMap<>();
      private final ConcurrentMap<String, URI> publishResults = new ConcurrentHashMap<>();
      private final ConcurrentMap<String, ArrayDeque<String>> publishOperations = new ConcurrentHashMap<>();

      public void value(String key, String value) {
        lookupValues.put(key, value);
      }

      private Optional<String> doLookup(String key) {
        lookupOperations.computeIfAbsent(key, k -> new AtomicInteger()).incrementAndGet();
        return Optional.ofNullable(lookupValues.get(key));
      }

      private String doPublish(String key, String value) {
        publishOperations.computeIfAbsent(key, k -> new ArrayDeque<>()).add(value);
        return value;
      }

      public List<String> published(String key) {
        return Optional.ofNullable(publishOperations.get(key))
            .map(ImmutableList::copyOf)
            .orElse(ImmutableList.of());
      }

      public int lookups(String key) {
        return Optional.ofNullable(lookupOperations.get(key)).map(AtomicInteger::get).orElse(0);
      }

      public void publish(String data, URI result) {
        publishResults.put(data, result);
      }
    }
  }

}
