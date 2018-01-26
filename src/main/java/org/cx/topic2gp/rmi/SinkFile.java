package org.cx.topic2gp.rmi;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.cx.topic2gp.core.Topic2FileConsumer;
import org.cx.topic2gp.util.Log;

import java.io.FileNotFoundException;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.*;

/**
 * RPC
 * Created by 冯曦 on 2017/12/19.
 */
public class SinkFile extends UnicastRemoteObject implements ISinkFile {

    public SinkFile() throws RemoteException {

    }

    /**
     * 默认3个线程消费
     * @param json  配置信息
     */
    @Override
    public void sinkFile(String json) throws RemoteException, FileNotFoundException {
        sinkFile(json, 3);
    }

    /**
     * @param json          配置信息
     * @param threadCount   消费者线程个数
     */
    @Override
    public void sinkFile(String json, int threadCount) throws RemoteException, FileNotFoundException {
        Map<String, String> config = getConfig(json);
        System.out.println(config);
        String topic = config.get("sinktopic");
        String tablename=config.get("tablename");
        Random random = new Random();
        Topic2FileConsumer consumer = new Topic2FileConsumer("g1399825311"+random.nextFloat(),topic , tablename, threadCount);
        consumer.consume();

    }

    @Override
    public String hello(String name) throws RemoteException {
        return "hello " + name;
    }

    private static Map<String, String> getConfig(String jsonStr) {
        JSONObject peiZhi = JSON.parseObject(jsonStr);
        JSONObject storage_type= (JSONObject) peiZhi.get("storage_type");

        Map<String, String> config = new HashMap<>();
        config.put("ip", (String) storage_type.get("ip"));
        config.put("port",(String) storage_type.get("port"));
        config.put("username", (String) storage_type.get("username"));
        config.put("password", (String) storage_type.get("password"));
        config.put("database", (String) storage_type.get("database"));
        config.put("tablename",storage_type.getString("tablename").toLowerCase());
        config.put("sinktopic", peiZhi.getJSONObject("kafka").getString("sink_topic").toLowerCase());
        Log.info("获取到的配置信息:" + config.toString());
        return config;
    }
}
