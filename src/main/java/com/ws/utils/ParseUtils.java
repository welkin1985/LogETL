package com.ws.utils;

import com.ws.bean.IPLocationBean;
import com.ws.bean.RequestBodyBean;
import com.ws.bean.TimeBean;
import com.ws.commone.ConfigManager;
import com.ws.constants.GlobalContants;
import redis.clients.jedis.Jedis;

import java.util.Calendar;

/**
 *
 */
public class ParseUtils {
    private static Jedis jedis = null;


    private static Jedis getConnect() {
        if (jedis == null) {
            synchronized (ParseUtils.class) {
                if (jedis == null) {
                    jedis = new Jedis(ConfigManager.getString(GlobalContants.IP_REDIS), 6379);
                }
            }
        }
        return jedis;
    }

    public static IPLocationBean parseIP(String ip, IPLocationBean ipLocationBean) {

        ipLocationBean.initValue();
        ipLocationBean.setIp(ip);

        String location = getConnect().get(ip.substring(0, ip.lastIndexOf(".") + 1) + "0");
        if (location != null && !"".equals(location)) {
            String[] localArray = location.split("\t");
            ipLocationBean.setIp(ip);
            ipLocationBean.setCountry(localArray[0].toLowerCase());
            ipLocationBean.setArea(localArray[1].toLowerCase());
            ipLocationBean.setProvince(localArray[2].toLowerCase());
            ipLocationBean.setCity(localArray[3].toLowerCase());
            ipLocationBean.setTelecom(localArray[4].toLowerCase());
            return ipLocationBean;
        } else {
            return ipLocationBean;
        }
    }

    public static TimeBean parseTime(String timeLog) {
        long timeStamp = Long.parseLong(timeLog.replaceAll(".", ""));
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(timeStamp);

        return null;
    }

    public static RequestBodyBean parseRequestBody(String bodyLog) {


        return null;
    }


}
