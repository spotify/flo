import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.Tuple;
import com.spotify.flo.EvalContext;
import com.spotify.flo.OperationExtractingContext;
import com.spotify.flo.Task;
import com.spotify.flo.TaskId;
import com.spotify.flo.TaskOperator;
import com.spotify.flo.context.MemoizingContext;
import com.spotify.flo.context.TaskBuilder;
import com.spotify.flo.context.TasksManifestBuilder;
import com.spotify.flo.context.Workflow;
import com.spotify.flo.context.WorkflowBuilder;
import com.spotify.flo.context.WorkflowManifest;
import com.spotify.flo.freezer.PersistingContext;
import com.spotify.flo.util.Date;
import com.spotify.flo.util.DateHour;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

public class Extract {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final ExecutorService EXECUTOR = new ForkJoinPool(32);

  public static void main(String[] args) throws IOException, ReflectiveOperationException {
    final URI manifestUri = URI.create(args[0]);
    final String param = args[1];

    final byte[] manifestBytes = Files.readAllBytes(Paths.get(manifestUri));
    final WorkflowManifest workflowManifest = OBJECT_MAPPER.readValue(manifestBytes, WorkflowManifest.class);

    final Path stagingLocation = Paths.get(workflowManifest.stagingLocation());
    final Path tasksStagingLocation = stagingLocation.resolve(
        "tasks-" + param + "-" + Long.toHexString(ThreadLocalRandom.current().nextLong()));

    final Path tempdir = Files.createTempDirectory(null, null);
    final Path tasksDir = Files.createTempDirectory(null, null);

    workflowManifest.files().stream()
        .map(f -> CompletableFuture.supplyAsync(() ->
            download(stagingLocation.resolve(f), tempdir), EXECUTOR))
        .forEach(CompletableFuture::join);

    final URLClassLoader workflowClassLoader = new URLClassLoader(new URL[]{tempdir.toUri().toURL()},
        ClassLoader.getSystemClassLoader());

    final Class<?> klass = workflowClassLoader.loadClass(workflowManifest.entryPoint().klass());
    final Method entrypoint = klass.getDeclaredMethod(workflowManifest.entryPoint().method());
    entrypoint.setAccessible(true);
    final String parameterType = workflowManifest.entryPoint().parameterType();
    final Object arg;
    if (parameterType.equals(Date.class.getName())) {
      arg = Date.parse(param);
    } else if (parameterType.equals(DateHour.class.getName())) {
      arg = DateHour.parse(param);
    } else if (parameterType.equals(String.class.getName())) {
      arg = param;
    } else {
      throw new IllegalArgumentException("Unsupported parameter type: " + parameterType);
    }

    final Task<?> root = (Task<?>) entrypoint.invoke(null, arg);

    stageWorkflow(tasksDir, root, tasksStagingLocation);
  }

  private static Path download(Path file, Path dst) {
    final Path destinationFile = dst.resolve(file.getFileName().toString());
    try {
      Files.copy(file, destinationFile, StandardCopyOption.REPLACE_EXISTING);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return destinationFile;
  }

  public static void stageWorkflow(Path dir, Task<?> root, Path tasksStagingLocation) {
    final PersistingContext persistingContext = new PersistingContext(dir, EvalContext.sync());

    final Workflow workflow = workflow(root);
    final File workflowJsonFile = dir.resolve("workflow.json").toFile();
    try {
      OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValue(workflowJsonFile, workflow);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    try {
      MemoizingContext.composeWith(persistingContext)
          .evaluate(root).toFuture().exceptionally(t -> null).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }

    stageTasks(workflow, root, workflowJsonFile, persistingContext, tasksStagingLocation);
  }

  private static Workflow workflow(Task<?> root) {
    final List<Task<?>> tasks = new ArrayList<>();
    final Set<TaskId> visited = new HashSet<>();
    enumerateTasks(root, tasks, visited);

    final WorkflowBuilder builder = new WorkflowBuilder();
    for (Task<?> task : tasks) {
      final Optional<? extends TaskOperator<?, ?, ?>> operator = OperationExtractingContext.operator(task);
      final TaskBuilder taskBuilder = new TaskBuilder()
          .operator(operator.map(o -> o.getClass().getName()).orElse("<generic>"))
          .id(task.id().toString())
          .payloadBase64(Base64.getEncoder().encodeToString(PersistingContext.serialize(task)));
      for (Task<?> upstream : task.inputs()) {
        taskBuilder.addUpstream(upstream.id().toString());
      }
      builder.addTask(taskBuilder.build());
    }

    return builder.build();
  }

  private static void enumerateTasks(Task<?> task, List<Task<?>> tasks, Set<TaskId> visited) {
    if (!visited.add(task.id())) {
      return;
    }
    for (Task<?> input : task.inputs()) {
      enumerateTasks(input, tasks, visited);
    }
    tasks.add(task);
  }

  private static void stageTasks(Workflow workflow, Task<?> root, File workflowJsonFile,
      PersistingContext persistingContext, Path tasksStagingLocation) {

    final TasksManifestBuilder manifestBuilder = new TasksManifestBuilder();

    final Map<TaskId, String> stagedTaskFiles = persistingContext.getFiles().entrySet().stream()
        .map(e -> CompletableFuture.supplyAsync(() -> {
          final TaskId id = e.getKey();
          final Path source = e.getValue();
          final String name = PersistingContext.cleanForFilename(id);
          final Path target = tasksStagingLocation.resolve(name);
          try {
            Files.copy(source, target);
          } catch (IOException ex) {
            throw new RuntimeException(ex);
          }
          return Tuple.of(id, name);
        }, EXECUTOR))
        .map(CompletableFuture::join)
        .collect(Collectors.toMap(Tuple::x, Tuple::y));

    stagedTaskFiles.forEach();
//    workflowFiles.add(workflowJsonFile.toPath());

//    final Map<TaskId, Path> taskFileMap = persistingContext.getFiles();
//    final List<TaskId> taskIds = taskFileMap.keySet().stream()
//        .sorted(Comparator.comparing(Object::toString))
//        .collect(toList());
//    final List<Path> taskPaths = taskIds.stream().map(taskFileMap::get).collect(toList());
//
//    final List<StagedPackage> stagedPackages = StagingUtil.stageClasspathElements(
//        toStrings(workflowFiles), stagingLocation.toString());
//
//    final List<StagedPackage> stagedTaskFiles = StagingUtil.stageClasspathElements(
//        toStrings(taskPaths), stagingLocation.toString());
//
//    final StagedPackage stagedWorkflowFile = StagingUtil.stageClasspathElements(
//        toStrings(Collections.singletonList(workflowJsonFile.toPath())), stagingLocation.toString()).get(0);
//
//    manifestBuilder.workflowFile(stagedWorkflowFile.name());
//    stagedPackages.stream().map(StagedPackage::name).forEach(manifestBuilder::addFile);
//    stagedTaskFiles.stream().map(StagedPackage::name).forEach(manifestBuilder::addFile);
//
//    for (int i = 0; i < taskIds.size(); i++) {
//      final TaskId taskId = taskIds.get(i);
//      final String name = stagedTaskFiles.get(i).name();
//      manifestBuilder.putTaskFile(taskId.toString(), name);
//    }
//
//    final TasksManifest manifest = manifestBuilder.build();
//
//    final String manifestName = "manifest-" + ThreadLocalRandom.current().nextLong() +".json";
//
//    try {
//      Files.write(Paths.get(stagingLocation).resolve(manifestName),
//          OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsBytes(manifest));
//    } catch (IOException e) {
//      throw new RuntimeException(e);
//    }

  }
}
