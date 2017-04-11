package com.liutaoyxz.zookeeper.Connect;

import org.apache.zookeeper.*;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by ltlxy on 2017/4/11.
 */
public class TestClientTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestClientTest.class);

    private ZooKeeper zooKeeper;

    private String connectStr;

    private int timeout = 15000;

    @Before
    public void init() {
        TestClient client = new TestClient();
        this.connectStr = "192.168.162.128:2181";
        try {
            this.zooKeeper = new ZooKeeper(connectStr, timeout, client);
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
        String s = this.zooKeeper.create(path, bytes, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
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
        this.zooKeeper.create(path, bytes, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, callback, null);
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


}