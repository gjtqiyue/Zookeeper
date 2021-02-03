import org.apache.zookeeper.*;
import org.apache.zookeeper.server.watch.WatcherMode;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;
import java.util.logging.Logger;

public class DistWorker implements Watcher {
    private final String workersPath = "/dist30/workers";
    private final String tasksPath = "/dist30/tasks";
    private final String masterPath = "/dist30/master";
    private final Logger logger = Logger.getLogger(DistWorker.class.getName());
    ZooKeeper zk;
    String task;
    String pinfo;

    public DistWorker(String pInfo) {
        this.pinfo = pInfo;
    }

    public void init(ZooKeeper zk) {
        // add a watch to master node
        // whenever a process creates a worker node it will notify master
        // if the master's state changes this should be notified
        this.zk = zk;
        zk.getData(pinfo, this, (rc, path, ctx, data, stat) -> {}, null);
        logger.info("DISTWORKER: add watch to worker node " + pinfo);
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        logger.info("DISTWORKER : " + pinfo + " Event received : " + watchedEvent);
        if (watchedEvent.getType() == Event.EventType.NodeDataChanged) {
            if (watchedEvent.getPath().equals(pinfo)) {
                // a new task has been assigned to this worker
                // retrive task's fullname
                zk.getData(pinfo, false,
                    (rc, path, ctx, taskPath, stat)->{
                        Thread t = new Thread(() -> {
                            compute(new String(taskPath, StandardCharsets.UTF_8));
                        }
                        );
                        t.start();
                    }, null);
            }
        }
    }

    private String getTaskName() {
        try {
            byte[] taskNameSerial = zk.getData(pinfo + "/task", false, null);
            String name = new String(taskNameSerial, StandardCharsets.UTF_8);
            return name;
        }
        catch (KeeperException e) { e.printStackTrace(); }
        catch (InterruptedException e) { e.printStackTrace(); }
        return "";
    }

    public void compute(String taskSerial) {
        //Execute the task.
        try {
            // Re-construct our task object
            logger.info("getting data from + " + task);
            byte[] taskData = zk.getData(taskSerial, false, null);
            ByteArrayInputStream bis = new ByteArrayInputStream(taskData);
            ObjectInput in = new ObjectInputStream(bis);
            DistTask dt = (DistTask) in.readObject();

            logger.info("DISTWORKER: " + pinfo + " computing...... ");
            dt.compute();

            // Serialize our Task object back to a byte array!
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(dt);
            oos.flush();
            byte[] result = bos.toByteArray();

            // Store it inside the result node.
            logger.info("DISWORKER: " + pinfo + " storing results......");
            zk.create(taskSerial + "/result", result, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, (rc, path, ctx, name)->{
                try {
                    logger.info(pinfo + " Storing results finished, idle...");
                    deleteTask();
                    zk.getData(pinfo, this, (rc1, path1, ctx1, data ,stat) -> {}, null);
                }
                catch (KeeperException e) { e.printStackTrace(); }
                catch (InterruptedException e) { e.printStackTrace(); }
            }, null);
        }
        catch(IOException io){System.out.println(io);}
        catch(ClassNotFoundException cne){System.out.println(cne);} catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }

    // once finish the task, delete this task
    private void deleteTask() throws KeeperException, InterruptedException {
        zk.setData(pinfo, "".getBytes(), -1, (rc, path, ctx, data) ->{}, null);
    }
}
