import org.apache.zookeeper.*;
import org.apache.zookeeper.server.watch.WatcherMode;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;
import java.util.logging.Logger;

public class DistMaster implements Watcher {
    private final String workersPath = "/dist30/workers";
    private final String tasksPath = "/dist30/tasks";
    private final Logger logger = Logger.getLogger(DistMaster.class.getName());
    Map<String, String> workers;
    Queue<String> pendingTasks;
    Set<String> registeredTasks;
    ZooKeeper zk;
    String pinfo;

    public DistMaster(String pinfo) {
        this.pinfo = pinfo;
        workers = new ConcurrentHashMap<>();
        registeredTasks = Collections.newSetFromMap(new ConcurrentHashMap<>());
        pendingTasks = new LinkedBlockingQueue<>();
    }

    public void init(ZooKeeper zk) {
        this.zk = zk;
        zk.getChildren(workersPath, this, (rc, path, ctx, list) -> threadedRun(this::checkToAddNewWorker, list), null);
        zk.getChildren(tasksPath, this, (i, s, o, list) -> threadedRun(this::tryAssignTask, list), null);
        logger.info("checking worker");
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        logger.info("DISTMASTER : Event received : " + watchedEvent);
        if (watchedEvent.getType() == Event.EventType.NodeChildrenChanged) {
            if (watchedEvent.getPath().equals(workersPath)) {
                logger.info("DISTMASTER: Worker change detected");
                zk.getChildren(workersPath, this, (rc, path, ctx, list) -> threadedRun(this::checkToAddNewWorker, list), null);
            } else if (watchedEvent.getPath().equals(tasksPath)) {
                logger.info("DISTMASTER: task change detected");
                zk.getChildren(tasksPath, this, (i, s, o, list) -> threadedRun(this::tryAssignTask, list), null);
            } else if (watchedEvent.getPath().matches(workersPath + "/worker-.+")) {
                zk.getChildren(watchedEvent.getPath(), this, (i, s, o, list) -> {
                    if (list.size() == 0) {
                        // this means the worker is done, 0 children
                        // remove the original task
                        String worker = watchedEvent.getPath();
                        String finishedTask = workers.get(worker);
                        registeredTasks.remove(finishedTask);
                        logger.info("task: " + finishedTask + " is finished");
                        threadedRun(this::tryAssignPendingTask, worker);
                    }
                }, null);
            }
        } else if (watchedEvent.getType() == Event.EventType.NodeDeleted) {
            if (watchedEvent.getPath().matches(workersPath + "/worker-.+")) {
                workers.remove(watchedEvent.getPath());
            }
        }
    }

    private <T> void threadedRun(Consumer<T> func, T argument) {
        Thread task = new Thread(() -> {func.accept(argument);});
        task.start();
    }

    private void tryAssignPendingTask(String worker) {
        String task = pendingTasks.poll();
        if (task != null) {
            logger.info("DISTMASTER: Add pending task " + task + " to " + worker);
            assignWork(worker, task);
            workers.put(worker, task);
        } else {
            // no pending task, worker becomes idle
            logger.info("DISTMASTER: No pending task, " + worker + " becomes idle");
            workers.put(worker, "");
        }
    }

    private synchronized void checkToAddNewWorker(List<String> children) {
        for (String child : children) {
            String workerFullName = workersPath + "/" + child;
            if (!workers.containsKey(workerFullName)) {
                logger.info("DISTMASTER: Add new Worker, " + workerFullName);
                workers.put(workerFullName, "");
                zk.addWatch(workerFullName, this, AddWatchMode.PERSISTENT, (rc, path, ctx) -> {}, null);
                tryAssignPendingTask(workerFullName);
            }
        }
    }

    private void removeTask(String task) {
        registeredTasks.remove(task);
    }

    private synchronized void tryAssignTask(List<String> children){
        for (String child : children) {
            String taskFullName = tasksPath + "/" + child;
            if (!registeredTasks.contains(taskFullName)) {
                // add the task to registered list
                registeredTasks.add(taskFullName);
                boolean isProcessed = false;
                for (Map.Entry<String, String> worker : workers.entrySet()) {
                    if (worker.getValue().equals("")) {
                        // worker is idle, can be used to assign tasks
                        worker.setValue(taskFullName);
                        assignWork(worker.getKey(), taskFullName);
                        isProcessed = true;
                        logger.info("Assign task: " + taskFullName + " to worker " + worker.getKey());
                        break;
                    }
                }

                // no idle worker, add it to waiting queue
                if (!isProcessed) {
                    logger.info("No available worker, put the task " + taskFullName + " to pending queue");
                    pendingTasks.offer(taskFullName);
                }
            }
        }
    }

    private void assignWork(String worker, String task)  {
        // assign task to the worker
        System.out.println("assign work and create node " + worker);
        zk.create(worker + "/task", task.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL, (rc, path, ctx, name, stat) -> {
                    logger.info("DISTMASTER: task has been created");
                }, null);
    }
}
