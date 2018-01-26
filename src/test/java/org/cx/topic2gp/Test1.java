package org.cx.topic2gp;

import ch.ethz.ssh2.crypto.digest.MD5;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.cx.topic2gp.rmi.ISinkFile;
import org.cx.topic2gp.util.Constants;
import java.io.*;

import junit.framework.TestCase;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.output.XMLOutputter;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * Created by 冯曦 on 2017/12/19.
 */
public class Test1 extends TestCase {

    public void test1(){
        String tableName = "aaa";
        String fileName = "load100W.txt";
        String jsonStr = "{\"schema\":{\"type\":\"struct\",\"fields\":[{\"type\":\"int32\",\"optional\":false,\"field\":\"id\"},{\"type\":\"string\",\"optional\":true,\"field\":\"name\"},{\"type\":\"int32\",\"optional\":true,\"field\":\"age\"}]},\"payload\":{\"id\":2,\"name\":\"zz\",\"age\":22}}";
        JSONObject info = JSON.parseObject(jsonStr);
        JSONObject schema = (JSONObject) info.get("schema");
        JSONArray fields = (JSONArray) schema.get("fields");
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE EXTERNAL TABLE ext_"+tableName+" (");
        for(int i=0;i<fields.size();i++) {
            JSONObject obj = (JSONObject) fields.get(i);
            String colName = (String) obj.get("field");
            boolean optional = (boolean) obj.get("optional");
            String type = (String) obj.get("type");
            if (i != fields.size() - 1) {
                sb.append(colName + " " + Constants.colType.get(type)+",");
            }else{
                sb.append(colName + " " + Constants.colType.get(type)+")");
            }
        }
        sb.append("LOCATION ('gpfdist://192.168.8.171:1234/" + fileName + "')"+
                " FORMAT 'TEXT' (DELIMITER '|') ENCODING 'UTF-8';"+
                " insert into "+tableName+" select * from ext_"+tableName+";"+
                " DROP EXTERNAL TABLE ext_"+tableName+";\"");
        System.out.println(fields);
        System.out.println(sb.toString());
    }

    public void test2(){
        StringBuilder finalMsg = new StringBuilder();
        String jsonStr = "{\"schema\":{\"type\":\"struct\",\"fields\":[{\"type\":\"int32\",\"optional\":false,\"field\":\"id\"},{\"type\":\"string\",\"optional\":true,\"field\":\"name\"},{\"type\":\"int32\",\"optional\":true,\"field\":\"age\"}]},\"payload\":{\"id\":2,\"name\":\"zz\",\"age\":22}}";
        JSONObject info = JSON.parseObject(jsonStr);
        JSONObject payload = (JSONObject) info.get("payload");
        StringBuilder sb = new StringBuilder();
    }

    public void test3(){
        String jsonStr = "{\"schema\":{\"type\":\"struct\",\"fields\":[{\"type\":\"int32\",\"optional\":false,\"field\":\"id\"},{\"type\":\"string\",\"optional\":true,\"field\":\"name\"},{\"type\":\"int32\",\"optional\":true,\"field\":\"age\"}]},\"payload\":{\"id\":2,\"name\":\"zz\",\"age\":22}}";
        JSONObject info = JSON.parseObject(jsonStr);
        JSONObject schema = (JSONObject) info.get("schema");
        JSONArray fields = (JSONArray) schema.get("fields");
        String[] fidldArr=new String[fields.size()];
        for(int i=0;i<fields.size();i++) {
            JSONObject obj = (JSONObject) fields.get(i);
            fidldArr[i] = (String) obj.get("field");
        }
    }

    public void test4() throws RemoteException, NotBoundException, MalformedURLException {
        String json = "{" +
                "    \"datasource\": \"code\"," +
                "    \"access_type\": {" +
                "        \"type\": \"1\"," +
                "        \"info\": {" +
                "            \"ip\": \"192.168.8.169\"," +
                "            \"port\": \"1521\"," +
                "            \"username\": \"tathata\"," +
                "            \"password\": \"tathata\"," +
                "            \"filepath\": \"/run/xxxx\"," +
                "            \"filetype\": \"zip\"," +
                "            \"seperator\": \"\\t\"," +
                "            \"sid\": \"orcl\"," +
                "            \"tablename\": \"WA_SOURCE_001\"" +
                "        }" +
                "    }," +
                "    \"storage_type\": {" +
                "        \"type\": \"GP\"," +
                "        \"ip\": \"192.168.8.191\"," +
                "        \"port\": \"5432\"," +
                "        \"username\": \"gpadmin\"," +
                "        \"password\": \"gpadmin\"," +
                "        \"database\": \"xfile\"," +
                "        \"tablename\": \"WA_SOURCE_001\"," +
                "        \"indexname\": \"index\"" +
                "    }," +
                "    \"protocol\": {" +
                "        \"enname\": \"WA_SOURCE_001\"," +
                "        \"cnname\": \"网页论坛\"," +
                "        \"type\": \"基础数据集\"," +
                "        \"columns\": [" +
                "            {" +
                "                \"name\": \"H001001\"," +
                "                \"index\": \"2\"," +
                "                \"check\": [" +
                "                    {" +
                "                        \"validate\": \"^[一-龥0-9]{1,20}$\"" +
                "                    }" +
                "                ]," +
                "                \"istransfer\": \"false\"," +
                "                \"isdistinct\": \"false\"" +
                "            }," +
                "            {" +
                "                \"name\": \"H001002\"," +
                "                \"index\": \"0\"," +
                "                \"check\": [" +
                "                    {" +
                "                        \"validate\": \"^[1-9]{1}[0-9]{0,20}$\"" +
                "                    }" +
                "                ]," +
                "                \"istransfer\": \"false\"," +
                "                \"isdistinct\": \"false\"" +
                "            }," +
                "            {" +
                "                \"name\": \"H001003\"," +
                "                \"index\": \"1\"," +
                "                \"check\": [" +
                "                    {" +
                "                        \"validate\": \"^[男|女|0-9]$\"" +
                "                    }" +
                "                ]," +
                "                \"istransfer\": \"true\"," +
                "                \"isdistinct\": \"false\"" +
                "            }" +
                "        ]" +
                "    }," +
                "    \"kafka\": {" +
                "        \"ip\": \"\"," +
                "        \"port\": \"\"," +
                "        \"username\": \"\"," +
                "        \"password\": \"\"," +
                "        \"source_topic\": \"source_WA_SOURCE_001\"," +
                "        \"sink_topic\": \"zz-out\"," +
                "        \"partitions\": \"3\"," +
                "        \"replications-factor\": \"1\"," +
                "        \"zookeeper_connect\": \"192.168.8.161:2181,192.168.8.163:2181,192.168.8.171:2181\"" +
                "    }" +
                "}";
        ISinkFile iSinkFile = (ISinkFile) Naming.lookup("rmi://localhost:9998/iSinkFile");
//        iSinkFile.sinkFile(json);
//        iSinkFile.hello("11");
    }

    public void test5() throws IOException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss-SSS");
        System.out.println(sdf.format(new Date()));
        Random random = new Random();
        System.out.println(random.nextFloat());

        File file = new File("D:\\用户目录\\Desktop\\TT.txt");
        System.out.println(file.getName());
        System.out.println(file.length());
        FileOutputStream fos = new FileOutputStream(file);
        fos.write("".getBytes());
        fos.flush();
        fos.close();
        System.out.println(file.length());
    }

    public void test7(){
        int i=2147483647;
        byte b = (byte) i;
        System.out.println(b);
    }

    public void test8(){
        String confJsonStr = "{" +
                "  \"datasource\":\"code\"," +
                "  \"access_type\":{" +
                "    \"type\":\"1\"," +
                "    \"info\":{" +
                "      \"ip\":\"192.168.8.169\"," +
                "      \"port\":\"1521\"," +
                "      \"username\":\"tathata\"," +
                "      \"password\":\"tathata\"," +
                "      \"filepath\":\"/run/xxxx\"," +
                "      \"filetype\":\"zip\"," +
                "      \"seperator\":\"\\t\"," +
                "      \"sid\":\"orcl\"," +
                "      \"tablename\":\"WA_SOURCE_001\"" +
                "    }" +
                "  }," +
                "  \"storage_type\":{" +
                "    \"type\":\"GP\"," +
                "    \"ip\":\"192.168.8.191\"," +
                "    \"port\":\"5432\"," +
                "    \"username\":\"gpadmin\"," +
                "    \"password\":\"gpadmin\"," +
                "    \"database\":\"xfile\"," +
                "    \"tablename\":\"WA_SOURCE_001\"," +
                "    \"indexname\":\"index\"" +
                "  }," +
                "  \"protocol\":{" +
                "    \"enname\":\"WA_SOURCE_001\"," +
                "    \"cnname\":\"网页论坛\"," +
                "    \"type\":\"基础数据集\"," +
                "    \"columns\":[" +
                "      {" +
                "        \"name\":\"H001001\"," +
                "        \"eng\":\"\"," +
                "        \"index\":\"2\"," +
                "        \"check\":[" +
                "          {" +
                "            \"validate\":\"^[\\u4e00-\\u9fa50-9]{1,20}$\"" +
                "          }" +
                "        ]," +
                "        \"istransfer\":\"false\"," +
                "        \"isdistinct\":\"false\"" +
                "      }," +
                "      {" +
                "        \"name\":\"H001002\"," +
                "        \"eng\":\"\"," +
                "        \"index\":\"0\"," +
                "        \"check\":[" +
                "          {" +
                "            \"validate\":\"^[1-9]{1}[0-9]{0,20}$\"" +
                "          }" +
                "        ]," +
                "        \"istransfer\":\"false\"," +
                "        \"isdistinct\":\"false\"" +
                "      }," +
                "      {" +
                "        \"name\":\"H001003\"," +
                "        \"eng\":\"\"," +
                "        \"index\":\"1\"," +
                "        \"check\":[" +
                "          {" +
                "            \"validate\":\"^[\\u7537|\\u5973|0-9]$\"" +
                "          }" +
                "        ]," +
                "        \"istransfer\":\"true\"," +
                "        \"isdistinct\":\"false\"" +
                "      }" +
                "    ]" +
                "  }," +
                "  \"kafka\":{" +
                "    \"ip\":\"192.168.8.161\"," +
                "    \"port\":\"22\"," +
                "    \"username\":\"root\"," +
                "    \"password\":\"runbaomi\"," +
                "    \"source_topic\":\"source_WA_SOURCE_001\"," +
                "    \"sink_topic\":\"zz-out\"," +
                "    \"partitions\":\"3\"," +
                "    \"replications-factor\":\"1\"," +
                "    \"zookeeper_connect\":\"192.168.8.161:2181,192.168.8.163:2181,192.168.8.171:2181\"" +
                "  }" +
                "}";
        JSONObject conf = JSON.parseObject(confJsonStr);
        JSONObject protocol = conf.getJSONObject("protocol");
        JSONArray columns = protocol.getJSONArray("columns");
        columns.sort(new Comparator<Object>() {
            //升序
            @Override
            public int compare(Object o1, Object o2) {
                int i1 = ((JSONObject) o1).getInteger("index");
                int i2 = ((JSONObject) o2).getInteger("index");
                return i1 - i2;
            }
        });
        for(int i=0;i<columns.size();i++) {
            System.out.println(columns.get(i));
        }
    }

    public void test9() throws IOException {
        String datasource = "WA_SOURCE";
        int count=0;
        String[] names = {"h11", "h22"};
        String[] engs={"aaa","bbb"};
        FileOutputStream fos = new FileOutputStream("D:\\用户目录\\Desktop\\aa.xml");


        Element root = new Element("MESSAGE");
        Document document = new Document(root);

        Element DATASET = new Element("DATASET");
        DATASET.setAttribute("name", "datasource");
        DATASET.setAttribute("rmk", "BCP FILE DATA STRUCURE");
        DATASET.setAttribute("fieldcnt", count + "");
        root.addContent(DATASET);
        Element DATA = new Element("DATA");
        DATASET.addContent(DATA);
        for(int i=0;i<names.length;i++) {
            Element ITEM = new Element("ITEM");
            ITEM.setAttribute("key", names[i]);
            ITEM.setAttribute("eng", engs[i]);
            ITEM.setAttribute("chn", "");
            ITEM.setAttribute("rmk", "");
            DATA.addContent(ITEM);
        }
        XMLOutputter xmlOutput = new XMLOutputter();
        xmlOutput.setEncoding("utf-8");
        xmlOutput.setNewlines(true);
        xmlOutput.setIndent("\t");
        xmlOutput.output(document,fos);

    }

    public void test10() throws IOException {
        List<File> files = new ArrayList<>();
        File file = new File("D:\\用户目录\\Desktop\\aa.xml");
        files.add(file);
        zipFiles(files,"D:\\用户目录\\Desktop\\aa.zip");
    }

    private static void zipFiles(List<File> files, String destFileStr) throws IOException {
        if (files.size() == 0) {
            return;
        }
        File zipFile = new File(destFileStr);
        ZipOutputStream zipOut = new ZipOutputStream(new FileOutputStream(zipFile));
        for (File file : files) {
            FileInputStream fis = new FileInputStream(file);
            zipOut.putNextEntry(new ZipEntry(file.getName()));
            byte[] bytes = new byte[2048];
            int temp = 0;
            while(-1 != (temp = fis.read(bytes))){
                zipOut.write(bytes,0,temp);
                zipOut.flush();
            }
            fis.close();
        }
        zipOut.close();
    }

    public void test11(){
        File file = new File("D:\\用户目录\\Desktop\\a\\b");
        file.mkdirs();
    }

    public void test12() throws IOException {
        File file = new File("D:\\用户目录\\Desktop\\a.txt");
        FileOutputStream fos = new FileOutputStream(file);
        fos.write("aaa".getBytes());
        FileInputStream fis = new FileInputStream(file);
        byte[] buf = new byte[1024];
        while (fis.read(buf) != -1) {
            System.out.println(new String(buf));
        }
    }

}
