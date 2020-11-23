import org.apache.zookeeper.*;
import org.apache.zookeeper.server.watch.WatcherMode;

import java.io.*;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;
import java.util.logging.Logger;

public class DistWorker implements Watcher {
    private final String workersPath = "/dist30/workers";
    private final String tasksPath = "/dist30/tasks";
    private final String masterPath = "/dist30/master";
    private final Logger logger = Logger.getLogger(DistMaster.class.getName());
    ZooKeeper zk;
    String task;
    String pinfo;

    public DistWorker(String pInfo) {
        pinfo = pinfo;
    }

    public void setZooKeeper(ZooKeeper zk) {
        this.zk = zk;
    }

    public void init() {
        // add a watch to master node
        // whenever a process creates a worker node it will notify master
        // if the master's state changes this should be notified
        zk.addWatch(pinfo, this, AddWatchMode.PERSISTENT, (rc, path, ctx) -> {}, null);
        logger.info("DISTWORKER: add watch to worker node " + pinfo);
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        logger.info("DISTWORKER : " + pinfo + " Event received : " + watchedEvent);
        if (watchedEvent.getType() == Event.EventType.NodeCreated) {
            if (watchedEvent.getPath().equals(pinfo+"/task")) {
                // a new task has been assigned to this worker
                // retrive task's fullname
                String c = getTaskName();
                zk.getData(tasksPath + c, false,
                    (rc, path, ctx, data, stat)->{
                        Thread t = new Thread(() -> { compute(data, tasksPath+c); });
                        t.start();
                    }, null);
            }
        }
    }

    private String getTaskName() {
        try {
            byte[] taskNameSerial = zk.getData(pinfo + "/task", false, null);
            ByteArrayInputStream bis = new ByteArrayInputStream(taskNameSerial);
            ObjectInput in = new ObjectInputStream(bis);
            String name = (String) in.readObject();
            return name;
        }
        catch (KeeperException e) { e.printStackTrace(); }
        catch (InterruptedException e) { e.printStackTrace(); }
        catch (IOException e) { e.printStackTrace(); }
        catch (ClassNotFoundException e) { e.printStackTrace(); }
        return "";
    }

    public void compute(byte[] taskSerial, String fullpath) {
        //Execute the task.
        try {
            // Re-construct our task object.
            ByteArrayInputStream bis = new ByteArrayInputStream(taskSerial);
            ObjectInput in = new ObjectInputStream(bis);
            DistTask dt = (DistTask) in.readObject();

            logger.info("DISTWORKER: " + pinfo + " computing...... ");
            dt.compute();

            // Serialize our Task object back to a byte array!
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(dt);
            oos.flush();
            taskSerial = bos.toByteArray();

            // Store it inside the result node.
            logger.info("DISWORKER: " + pinfo + " storing results......");
            zk.create(fullpath + "/result", taskSerial, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, (rc, path, ctx, name)->{
                try { deleteTask(); }
                catch (KeeperException e) { e.printStackTrace(); }
                catch (InterruptedException e) { e.printStackTrace(); }
            }, null);
        }
        catch(IOException io){System.out.println(io);}
        catch(ClassNotFoundException cne){System.out.println(cne);}
    }

    // once finish the task, delete this task
    private void deleteTask() throws KeeperException, InterruptedException {
        zk.delete(pinfo + "/task", -1);
    }
}
