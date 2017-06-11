/**
 *
 */
public class Demo {
    public static void main(String[] args) {
        String sep ="^A";
        String toTest = "114.245.34.37^A1497181542.611^A47.92.108.46:11111^A/log.gif?u_ud=BF532FAC-59EB-45BD-9979-A5488DAFE6A1&u_sd=86637B2A-E4DC-4FC4-AB12-ABB817262B89&u_mid=&c_time=1497181539162&l=zh-CN&b_iev=Mozilla/5.0%20(Windows%20NT%2010.0;%20Win64;%20x64)%20AppleWebKit/537.36%20(KHTML,%20like%20Gecko)%20Chrome/58.0.3029.96%20Safari/537.36&b_rst=2048*1152&ver=1.0&sdk=js&pl=website&en=e_pv&p_url=http%3A%2F%2Flocalhost%3A8080%2Fshop%2Fdemo01.jsp&p_ref=http%3A%2F%2Flocalhost%3A8080%2Findex.jsp&tt=demo01";
        String[] split = toTest.split(sep);
        System.out.println(sep);
        System.out.println(split.length);

    }
}
