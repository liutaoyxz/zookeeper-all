package com.liutaoyxz.zookeeper.Connect;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * Created by ltlxy on 2017/4/11.
 */
public class TestClientTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestClientTest.class);

    private ZooKeeper zk;

    private String connectStr;

    private int timeout = 15000;

    @Before
    public void init() {
        TestClient client = new TestClient();
        this.connectStr = "192.168.162.128:2181";
        try {
            this.zk = new ZooKeeper(connectStr, timeout, client);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void runForMaster() throws InterruptedException {
        while (true) {
            try {
                this.createPath("/master", "master data".getBytes());
            } catch (KeeperException.NodeExistsException e) {
                Thread.sleep(3000);
                LOGGER.info("主节点已经存在，轮询创建");
            } catch (KeeperException.ConnectionLossException e) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }

    private void createPath(String path, byte[] bytes) throws KeeperException, InterruptedException {
        String s = this.zk.create(path, bytes, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        LOGGER.info("创建节点--> {}", path);
    }


    @Test
    public void runForMasterAsync() throws InterruptedException {
        while (true) {
            this.createPathAsync("/master", "master data".getBytes());
        }

    }

    private void createPathAsync(String path, byte[] bytes) {
        AsyncCallback.StringCallback callback = new AsyncCallback.StringCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, String name) {
                switch (KeeperException.Code.get(rc)) {
                    case CONNECTIONLOSS:
                        LOGGER.info("连接错误");
                        break;
                    case NODEEXISTS:
//                        LOGGER.info("node exists");
                        break;
                    case OK:
                        LOGGER.info("创建节点--> {}", path);
                        break;
                    default:
                        break;
                }
            }
        };
        this.zk.create(path, bytes, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, callback, null);
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void getChildren() throws KeeperException, InterruptedException {
        Stat stat = new Stat();
        System.out.println(stat);
        List<String> children = zk.getChildren("/dubbo", true, stat);
        for (String s : children) {
            byte[] data = zk.getData("/dubbo/" + s, false, null);
            String dataStr = null;
            if (data != null){
                dataStr = new String(data);
            }
            System.out.println("/dubbo" + s + " -- >" + dataStr);
        }
        System.out.println(stat);
    }

    @Test
    public void getC() throws KeeperException, InterruptedException {
        this.getChildren();
    }

    @Test
    public void nodeCreated() throws InterruptedException {
        Watcher existsWatcher = new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                System.out.println("我自己监视到的事件 ----> "+event);
                String path = event.getPath();
                Event.EventType type = event.getType();
                switch (type){
                    case NodeCreated:
                        System.out.println(path +"被创建");
                        break;
                    case NodeDeleted:
                        System.out.println(path+"被删除");
                        break;
                    case NodeDataChanged:
                        System.out.println(path+"数据发生变化");
                        try {
                            byte[] data = zk.getData(path, false, new Stat());
                            System.out.println(new String(data));
                        } catch (KeeperException e) {
                            e.printStackTrace();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        break;
                }
                try {
                    zk.exists(path,this);
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        };
        AsyncCallback.StatCallback statCallback = new AsyncCallback.StatCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, Stat stat) {
                switch (KeeperException.Code.get(rc)){
                    case OK:
                        System.out.println(path+"节点存在!");
                        break;
                    case CONNECTIONLOSS:
                        System.out.println("连接都是,尝试重连......");
                        exists(path,existsWatcher,this);
                        break;
                    case NONODE:
                        System.out.println("节点不存在,不知道会不会监视!!");
                        break;
                    case NODEEXISTS:
                        System.out.println("也是节点存在,但是理论上不应该到这里!!!");
                        break;
                    default:
                        System.out.println(KeeperException.Code.get(rc));
                        System.out.println("以上状态都没有命中~~~");
                        break;
                }
            }
        };


        this.exists("/liutaoyxz",existsWatcher,statCallback);
        synchronized (TestClientTest.class){
            TestClientTest.class.wait();
        }
    }


    private void exists(String path, Watcher watch, AsyncCallback.StatCallback callback){

        zk.exists(path, watch,callback,path);
    }


}