package org.cx.topic2gp.core;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.cx.topic2gp.util.Constants;
import org.cx.topic2gp.util.Log;
import org.cx.topic2gp.util.PropUtil;
import org.cx.topic2gp.util.RemoteExecuteCommand;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.ConsumerTimeoutException;
import kafka.consumer.KafkaStream;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Queue;
import java.util.concurrent.TimeUnit;

/**
 * 消费者线程
 * Created by 冯曦 on 2017/12/17.
 */
public class Topic2FileThread extends Thread {

    private final KafkaStream kafkaStream;
    private final static String prePath = PropUtil.prop.getProperty("file.dir");
//    private final static String prePath = "D:\\用户目录\\Desktop\\test\\";
    private File file;
    private FileOutputStream fos;
    private BufferedOutputStream bos;
    private String topicName;
    private String tableName;
    private String[] fieldArr;
    private boolean flag =true;
    Queue<String> msgQueue;
    private JSONArray fields;

    private Object lock;

    public Topic2FileThread(KafkaStream kafkaStream, String threadName,
                            String topicName, String tableName, Object lock,
                            Queue<String> msgQueue)
            throws FileNotFoundException {

        this.topicName = topicName;
        this.kafkaStream = kafkaStream;
        this.setName(threadName);
        this.tableName = tableName;
        this.lock=lock;
        this.msgQueue=msgQueue;
    }

    @Override
    public void run() {
        ConsumerIterator<byte[], byte[]> consumerIt = kafkaStream.iterator();
        final SimpleDateFormat sdf = new SimpleDateFormat("yyyy_MM_dd_HH_mm_ss_SSS");
        file = new File(prePath + topicName + "-" +getName()+"-"+ sdf.format(new Date()) + ".txt");
        try {
            fos = new FileOutputStream(file);
            bos = new BufferedOutputStream(fos);
            while (consumerIt.hasNext()) {
                String msg = new String(consumerIt.next().message());
                if (msg == null || msg.equals("")) {
                    continue;
                }
                msgQueue.offer(msg);
                if (flag) {
                    synchronized (lock) {
                        fieldArr = getTableFields(msg);

                        JSONObject info = JSON.parseObject(msg);
                        JSONObject schema = (JSONObject) info.get("schema");
                        fields = (JSONArray) schema.get("fields");
                        flag = false;
                    }
                    new Thread() {
                        @Override
                        public void run() {
                            int count = 0;
                            int exitCount = 20;
                            int sleepSecond = 6 ;
                            while (true) {
                                String msg = "";
                                try {
                                    msg = msgQueue.poll();
                                    if (msg == null) {
                                        try {
                                            if (count > exitCount) {
                                                //退出线程
                                                bos.close();//bos.close()===bos.flush();fos.close();
                                                break;
                                            }
                                            TimeUnit.SECONDS.sleep(sleepSecond);
                                            count++;
                                            bos.flush();
                                            if (file.length() > 0) {
//                                                只有BufferedOutputStream.flush()才会刷新缓存，其它的不起作用
//                                                fos.flush();
                                                bos.close();
                                                Topic2FileThread.dumpFile(tableName,
                                                        Topic2FileThread.this.getName(), file.getName(), fields, sdf.format(new Date()));
                                                file = new File(prePath + topicName + "-" + getName() + "-" + sdf.format(new Date()) + ".txt");
                                                fos = new FileOutputStream(file);
                                                bos = new BufferedOutputStream(fos);
                                            }
                                        } catch (Exception e1) {
                                            e1.printStackTrace();
                                        }
                                        continue;
                                    }
                                    count = 0;//有新数据进来，重置状态
                                    String finalStr = deal(msg, fieldArr);
                                    double fileSize = Double.valueOf(file.length()) / 1024 / 1024;

                                    if (fileSize > 30) {
                                        bos.close();
                                        File tFile = file;
                                        new Thread(new Runnable() {
                                            @Override
                                            public void run() {
                                                Topic2FileThread.dumpFile(tableName,
                                                        Topic2FileThread.this.getName(), tFile.getName(), fields, sdf.format(new Date()));
                                            }
                                        }).start();
                                        file = new File(prePath + topicName + "-" + getName() + "-" + sdf.format(new Date()) + ".txt");
                                        fos = new FileOutputStream(file);
                                        bos = new BufferedOutputStream(fos);
                                    }
                                    bos.write(finalStr.getBytes());
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                            }
                        }
                    }.start();
                }
            }
        } catch (ConsumerTimeoutException e) {
            Log.info(topicName+"11长时间未收到消息，线程--"+getName()+"--结束.");
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * 将文件导入Greenplum
     * @param tableName     内部表名
     * @param threadName    线程名（避免外部表重复）
     * @param fileName
     * @param fields
     */
    private static void dumpFile(String tableName,String threadName,String fileName,JSONArray fields,String dateStr){

        StringBuilder sb = new StringBuilder();
        String exTableName=" ext_"+tableName+"_"+threadName+"_"+dateStr+" ";
        String gpfdistHost=PropUtil.prop.getProperty("gpfdist.host");
        String gpfdistPort = PropUtil.prop.getProperty("gpfdist.port");
        String greenplumHost=PropUtil.prop.getProperty("greenplum.host");
        sb.append("psql -d xfile -c \"CREATE EXTERNAL TABLE "+exTableName+" (");
        for(int i=0;i<fields.size();i++) {
            JSONObject obj = (JSONObject) fields.get(i);
            String colName = (String) obj.get("field");
            boolean optional = (boolean) obj.get("optional");
            String type = (String) obj.get("type");
            if (i != fields.size() - 1) {
                sb.append("T"+colName + " " + Constants.colType.get(type)+",");
            }else{
                sb.append("T"+colName + " " + Constants.colType.get(type)+")");
            }
        }
        sb.append("LOCATION ('gpfdist://"+gpfdistHost+":"+gpfdistPort+"/" + fileName + "')"+
                " FORMAT 'TEXT' (DELIMITER '|') ENCODING 'UTF-8';"+
                " insert into "+tableName+" ("+getTableFieldUpCase(fields)+") select "+getTableField(fields)+" from "+exTableName+";"+
                " DROP EXTERNAL TABLE "+exTableName+";\"");
        RemoteExecuteCommand command = new RemoteExecuteCommand(greenplumHost, "gpadmin", "gpadmin");
        String cmdsql = sb.toString();
        Log.info(cmdsql);
        command.execute(cmdsql);
    }

    /**
     * 转换topic中的数据格式，入到文件中的每一行
     * @param msg       topic中的一条消息
     * @param fieldArr  表中的每一列
     * @return
     */
    private static String deal(String msg,String[] fieldArr) {
        JSONObject info = JSON.parseObject(msg);
        JSONObject payload = (JSONObject) info.get("payload");
        StringBuilder sb = new StringBuilder();
        for(int i=0;i<fieldArr.length;i++) {
            String val = payload.getString(fieldArr[i]);
            if (i != fieldArr.length - 1) {
                sb.append(val + "|");
            }else{
                sb.append(val);
            }

        }sb.append("\n");
        return sb.toString();
    }

    /**
     *
     * @param jsonStr topic中的每条消息
     * @return
     */
    private static String[] getTableFields(String jsonStr) {
        JSONObject info = JSON.parseObject(jsonStr);
        JSONObject schema = (JSONObject) info.get("schema");
        JSONArray fields = (JSONArray) schema.get("fields");
        String[] fidldArr=new String[fields.size()];
        for(int i=0;i<fields.size();i++) {
            JSONObject obj = (JSONObject) fields.get(i);
            fidldArr[i] = (String) obj.get("field");
        }
        return fidldArr;
    }

    private static String getTableField(JSONArray fields) {
        StringBuilder sb = new StringBuilder();
        for(int i=0;i<fields.size();i++) {
            JSONObject obj = (JSONObject) fields.get(i);
            String colName = (String) obj.get("field");
            String type = (String) obj.get("type");
            if (i != fields.size() - 1) {
                sb.append("T"+colName + " ,");
            }else{
                sb.append("T"+colName + " ");
            }
        }
        return sb.toString().toLowerCase();

    }


    private static String getTableFieldUpCase(JSONArray fields) {
        StringBuilder sb = new StringBuilder();
        for(int i=0;i<fields.size();i++) {
            JSONObject obj = (JSONObject) fields.get(i);
            String colName = (String) obj.get("field");
            String type = (String) obj.get("type");
            if (i != fields.size() - 1) {
                sb.append("\\\"T"+colName + "\\\" ,");
            }else{
                sb.append("\\\"T"+colName + "\\\" ");
            }
        }
        return sb.toString().toUpperCase();

    }
}
