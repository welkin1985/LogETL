package com.ws.bean;

import com.ws.commone.ConfigManager;
import com.ws.constants.GlobalContants;

import java.io.Serializable;

/**
 *
 */
public class IPLocationBean implements Serializable {

    private String ip           = ConfigManager.getString(GlobalContants.UNKONWN);    //ip
    private String Country      = ConfigManager.getString(GlobalContants.UNKONWN);    //国家
    private String area         = ConfigManager.getString(GlobalContants.UNKONWN);    //区域
    private String province     = ConfigManager.getString(GlobalContants.UNKONWN);    //省份
    private String city         = ConfigManager.getString(GlobalContants.UNKONWN);    //城市
    private String telecom      = ConfigManager.getString(GlobalContants.UNKONWN);    //电信与运营商

    public IPLocationBean(){

    }

    public void initValue(){
        this.ip           = ConfigManager.getString(GlobalContants.UNKONWN);
        this.Country      = ConfigManager.getString(GlobalContants.UNKONWN);
        this.area         = ConfigManager.getString(GlobalContants.UNKONWN);
        this.province     = ConfigManager.getString(GlobalContants.UNKONWN);
        this.city         = ConfigManager.getString(GlobalContants.UNKONWN);
        this.telecom      = ConfigManager.getString(GlobalContants.UNKONWN);
    }




    public void setIp(String ip) {
        this.ip = ip;
    }

    public void setCountry(String country) {
        Country = country;
    }

    public void setArea(String area) {
        this.area = area;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public void setCity(String city) {
        this.city = city;
    }


    public void setTelecom(String telecom) {
        this.telecom = telecom;
    }

    public String getIp() {
        return ip;
    }

    public String getCountry() {
        return Country;
    }

    public String getArea() {
        return area;
    }

    public String getProvince() {
        return province;
    }

    public String getCity() {
        return city;
    }


    public String getTelecom() {
        return telecom;
    }


}
