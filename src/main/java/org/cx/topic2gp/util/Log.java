package org.cx.topic2gp.util;

import com.sun.org.apache.xpath.internal.SourceTree;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 日志打印
 * Created by 冯曦 on 2017/12/21.
 */
public class Log {

    public static void info(String log) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        System.out.println("------" + sdf.format(new Date()) + "---" + log);
    }
}
