package org.cx.topic2gp.util;

import java.io.IOException;
import java.util.Properties;

/**
 * Created by 冯曦 on 2017/12/26.
 */
public class PropUtil {
    public static final Properties prop;
    static{
        prop = new Properties();
        try {
            prop.load(PropUtil.class.getClassLoader().getResourceAsStream("conf.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
