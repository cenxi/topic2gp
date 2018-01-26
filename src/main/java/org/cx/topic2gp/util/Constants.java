package org.cx.topic2gp.util;

import org.apache.commons.collections.map.HashedMap;

import java.util.Map;

/**
 * 公用常量
 * Created by 冯曦 on 2017/12/19.
 */
public class Constants {

    public static final Map<String, String> colType = new HashedMap();

    static {
        colType.put("int32", "numeric");
        colType.put("string", "char(255)");
    }

    public static final String logfilePath = PropUtil.prop.getProperty("logFile.dir");

    public static final String standardLogfilePath = logfilePath+"\\standard";
    public static final String errorLogfilePath = logfilePath+"\\error";

}
