package com.liutaoyxz.zookeeper.Connect;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

/**
 * @author liutao
 * @description :
 * @create 2017-04-10 9:36
 */
public class TestClient implements Watcher {

    @Override
    public void process(WatchedEvent watchedEvent) {
        System.out.println(watchedEvent);
    }

}
