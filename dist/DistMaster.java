import org.apache.zookeeper.*;
import org.apache.zookeeper.server.watch.WatcherMode;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;

public class DistMaster implements Watcher {
    private final String workersPath = "/dist30/workers";
    private final String tasksPath = "/dist30/tasks";

    Map<String, Boolean> workers;
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
        zk.getChildren(tasksPath, this, (i, s, o, list) -> threadedRun(this::tryAssignTask, list), null);
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        System.out.println("DISTMASTER : Event received : " + watchedEvent);
        if (watchedEvent.getType() == Event.EventType.NodeChildrenChanged) {
            if (watchedEvent.getPath().equals(workersPath)) {
                System.out.println("DISTMASTER: Worker change detected");
                zk.getChildren(workersPath, this, (rc, path, ctx, list) -> threadedRun(this::checkToAddNewWorker, list), null);
            } else if (watchedEvent.getPath().equals(tasksPath)) {
                System.out.println("DISTMASTER: task change detected");
                zk.getChildren(tasksPath, this, (i, s, o, list) -> threadedRun(this::tryAssignTask, list), null);
            } else if (watchedEvent.getPath().matches(workersPath + "/worker-.+")) {
                zk.getChildren(tasksPath, this, (i, s, o, list) -> {
                    if (list.size() == 0) {
                        // this means the worker is done
                        threadedRun(this::tryAssignPendingTask, watchedEvent.getPath());
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
            System.out.println("DISTMASTER: Add pending task " + task + " to " + worker);
            assignWork(worker, task);
        } else {
            // no pending task, worker becomes idle
            System.out.println("DISTMASTER: No pending task, " + worker + " becomes idle");
            workers.put(worker, true);
        }
    }

    private synchronized void checkToAddNewWorker(List<String> children) {
        for (String child : children) {
            String workerFullName = workersPath + "/" + child;
            if (!workers.containsKey(workerFullName)) {
                workers.put(workerFullName, true);
                zk.addWatch(workerFullName, this, AddWatchMode.PERSISTENT, (rc, path, ctx) -> {}, null);
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
                for (Map.Entry<String, Boolean> worker : workers.entrySet()) {
                    if (worker.getValue()) {
                        // worker is idle, can be used to assign tasks
                        worker.setValue(false);
                        assignWork(worker.getKey(), taskFullName);
                        isProcessed = true;
                        break;
                    }
                }

                // no idle worker, add it to waiting queue
                if (!isProcessed) {
                    pendingTasks.offer(taskFullName);
                }
            }
        }
    }

    private void assignWork(String worker, String task)  {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(task);
            oos.flush();
            // assign task to the worker
            zk.create(worker + "/task", bos.toByteArray(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.EPHEMERAL, (i, s, o, s1) -> {
                        zk.addWatch(s1,  (e) -> {
                            if (e.getType() == Event.EventType.NodeDeleted) removeTask(e.getPath());
                        }, AddWatchMode.PERSISTENT, (i1, s2, o1) -> {},null);
                    }, null);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
