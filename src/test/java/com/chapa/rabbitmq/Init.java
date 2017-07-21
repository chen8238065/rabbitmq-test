package com.chapa.rabbitmq;

import java.io.IOException;
import java.util.Properties;

/**
 * Created by chapa on 16-8-19.
 */
public class Init {
    static Properties pro = new Properties();

    static {
        try {
            pro.load(Init.class.getResourceAsStream("/rabbitmq.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String server = (String) pro.get("server");
    public static Integer port = Integer.valueOf(pro.getProperty("port"));
    public static String url = server + ":" + port;
}
