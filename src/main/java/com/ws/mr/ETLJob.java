package com.ws.mr;

import com.ws.bean.IPLocationBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapreduce.OrcOutputFormat;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.net.URLDecoder;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;


public class ETLJob extends Configured implements Tool {

    /*MapClass静态类*/
    public static class MapClass extends Mapper<LongWritable, Text, NullWritable, OrcStruct> {

        //日志项
        private static Logger log = Logger.getLogger(MapClass.class);

        //参数字段
        private String sepInLog = "\t";
        private String sepInBody = "&";
        private int infoNum = 4;

        //bean载体
        private IPLocationBean ipLocation = new IPLocationBean();
        private Calendar cal = Calendar.getInstance();

        //redis 连接
        private Jedis jedis = new Jedis("node00.ali.ws.com",6379);

        //字段配置
        private static Map<String,Integer> fieldMap = new HashMap<String, Integer>(){};
        static {

            fieldMap.put("en",      16);
            fieldMap.put("ver",     17);
            fieldMap.put("pl",      18);
            fieldMap.put("sdk",     19);
            fieldMap.put("u_ud",    20);
            fieldMap.put("u_mid",   21);
            fieldMap.put("u_sd",    22);
            fieldMap.put("c_time",  23);
            fieldMap.put("l",       24);
            fieldMap.put("b_iev",   25);
            fieldMap.put("b_rst",   26);
            fieldMap.put("p_url",   27);
            fieldMap.put("p_ref",   28);
            fieldMap.put("tt",      29);
            fieldMap.put("oid",     30);
            fieldMap.put("on",      31);
            fieldMap.put("cua",     32);
            fieldMap.put("cut",     33);
            fieldMap.put("pt",      34);
            fieldMap.put("ca",      35);
            fieldMap.put("ac",      36);
            fieldMap.put("kv_",     37);
            fieldMap.put("du",      38);
        }


        //orc输出配置
	    private TypeDescription schema = TypeDescription.fromString(
	            "struct<"
                        +"ip"            +":string,"
                        +"country"       +":string,"
                        +"area"          +":string,"
                        +"province"      +":string,"
                        +"city"          +":string,"
                        +"telecom"       +":string,"

                        +"timeStamp"     +":string,"
                        +"year"          +":string,"
                        +"quarter"       +":string,"
                        +"month"         +":string,"
                        +"monthWeek"     +":string,"
                        +"weekDay"       +":string,"
                        +"day"           +":string,"
                        +"hour"          +":string,"
                        +"minis"         +":string,"
                        +"second"        +":string,"


                        +"eventName"        +":string,"
                        +"version"          +":string,"
                        +"platform"         +":string,"
                        +"sdk"              +":string,"
                        +"uuid"             +":string,"
                        +"memberId"         +":string,"
                        +"sessionId"        +":string,"
                        +"clientTime"       +":string,"
                        +"language"         +":string,"
                        +"userAgent"        +":string,"
                        +"resolution"       +":string,"
                        +"currentUrl"       +":string,"
                        +"referrerUrl"      +":string,"
                        +"title"            +":string,"
                        +"orderId"          +":string,"
                        +"orderName"        +":string,"
                        +"currencyAmount"   +":string,"
                        +"currencyType"     +":string,"
                        +"paymentType"      +":string,"
                        +"category"         +":string,"
                        +"action"           +":string,"
                        +"kv"               +":string,"
                        +"duration"         +":string"

                        +">"
        );
	    private OrcStruct orcs = (OrcStruct) OrcStruct.createValue(schema);





        /**
         * ETL处理HDFS上的原始nginx采集的accessLog日志
         * @param key
         * @param value
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] infos = value.toString().split(sepInLog);

            System.out.println("======================================="+value.toString());
            System.out.println("======================================="+infos.length);

            if (infos.length == infoNum) {


                //  解析IP  info[0]   //////////////////////////////////

                ipLocation.initValue();
//                ipLocation = ParseUtils.parseIP(infos[0],ipLocation);


                String location = jedis.get(infos[0].substring(0, infos[0].lastIndexOf(".") + 1) + "0");
                if (location != null && !"".equals(location)) {
                    String[] localArray = location.split("\t");
                    ipLocation.setIp(infos[0]);
                    ipLocation.setCountry(localArray[0].toLowerCase());
                    ipLocation.setArea(localArray[1].toLowerCase());
                    ipLocation.setProvince(localArray[2].toLowerCase());
                    ipLocation.setCity(localArray[3].toLowerCase());
                    ipLocation.setTelecom(localArray[4].toLowerCase());
                }


                orcs.setFieldValue(0,new Text(ipLocation.getIp()));
                orcs.setFieldValue(1,new Text(ipLocation.getCountry()));
                orcs.setFieldValue(2,new Text(ipLocation.getArea()));
                orcs.setFieldValue(3,new Text(ipLocation.getProvince()));
                orcs.setFieldValue(4,new Text(ipLocation.getCity()));
                orcs.setFieldValue(5,new Text(ipLocation.getTelecom()));


                //  解析日期  info[1]  ////////////////////////////////

                cal.setTimeInMillis(Long.parseLong(infos[1].replace(".","")));

                orcs.setFieldValue(6, new Text(String.valueOf(cal.getTimeInMillis())));             //时间戳
                orcs.setFieldValue(7, new Text(String.valueOf(cal.get(Calendar.YEAR))));            //年
                orcs.setFieldValue(8, new Text(String.valueOf(cal.get(Calendar.MONTH)/3+1)));       //季度
                orcs.setFieldValue(9,new Text(String.valueOf(cal.get(Calendar.MONTH)+1)));         //月
                orcs.setFieldValue(10,new Text(String.valueOf(cal.get(Calendar.WEEK_OF_MONTH))));   //月周
                orcs.setFieldValue(11,new Text(String.valueOf(cal.get(Calendar.DAY_OF_WEEK))));     //周几
                orcs.setFieldValue(12,new Text(String.valueOf(cal.get(Calendar.DAY_OF_MONTH))));    //日期
                orcs.setFieldValue(13,new Text(String.valueOf(cal.get(Calendar.HOUR))));            //时
                orcs.setFieldValue(14,new Text(String.valueOf(cal.get(Calendar.MINUTE))));          //分
                orcs.setFieldValue(15,new Text(String.valueOf(cal.get(Calendar.SECOND))));          //秒



                //  URI  字段初始化  ////////////////////////////////
                for (int i = 16; i <= 38; i++) {
                    orcs.setFieldValue(i,new Text("-"));
                }


                String request = URLDecoder.decode(infos[3], "UTF-8");
                //判断是否能够解析
                if(request.contains("?") && request.contains(sepInBody)){
                    String[] bodyArray = request.substring(request.indexOf("?")+1).split(sepInBody);
                    //遍历uri，提取字段和内容
                    for (String body : bodyArray) {
                        String[] kv = body.split("=");
                        String requestKey = kv[0];
                        String requestVal = (kv.length==2)?kv[1]:"-";


                        if( !"".equals(requestKey) && fieldMap.get(requestKey)!=null){
                            orcs.setFieldValue(fieldMap.get(requestKey),new Text(requestVal));
                        }else {
                            context.getCounter("error","未知请求字段").increment(1L);
                            log.warn("未知请求字段"+requestKey+" | "+value.toString());
                        }
                    }
                    context.write(NullWritable.get(), orcs);

                }else {
                    context.getCounter("error","请求url格式错误,无=或？ :").increment(1L);
                    log.warn("请求url格式错误: "+request+" | "+value.toString());
                }
            } else {
                context.getCounter("error","日志信息按分割符分割后，长度不足").increment(1L);

                log.warn("日志信息按分割符分割后，长度不足: "+sepInLog+"   "+value.toString());
            }
        }
    }



    /*driver驱动方法*/
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        conf.set("orc.mapred.output.schema","struct<"

                        +"ip"               +":string,"
                        +"country"          +":string,"
                        +"area"             +":string,"
                        +"province"         +":string,"
                        +"city"             +":string,"
                        +"telecom"          +":string,"
                        +"timeStamp"        +":string,"
                        +"year"             +":string,"
                        +"quarter"          +":string,"
                        +"month"            +":string,"
                        +"monthWeek"        +":string,"
                        +"weekDay"          +":string,"
                        +"day"              +":string,"
                        +"hour"             +":string,"
                        +"minis"            +":string,"
                        +"second"           +":string,"
                        +"eventName"        +":string,"
                        +"version"          +":string,"
                        +"platform"         +":string,"
                        +"sdk"              +":string,"
                        +"uuid"             +":string,"
                        +"memberId"         +":string,"
                        +"sessionId"        +":string,"
                        +"clientTime"       +":string,"
                        +"language"         +":string,"
                        +"userAgent"        +":string,"
                        +"resolution"       +":string,"
                        +"currentUrl"       +":string,"
                        +"referrerUrl"      +":string,"
                        +"title"            +":string,"
                        +"orderId"          +":string,"
                        +"orderName"        +":string,"
                        +"currencyAmount"   +":string,"
                        +"currencyType"     +":string,"
                        +"paymentType"      +":string,"
                        +"category"         +":string,"
                        +"action"           +":string,"
                        +"kv"               +":string,"
                        +"duration"         +":string"
                +">");
        Job job = Job.getInstance(conf, "logETL");
        job.setJarByClass(ETLJob.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
//        FileInputFormat.setInputPaths(job, new Path("d:/etl_input"));
//        FileOutputFormat.setOutputPath(job, new Path("d:/etl_output"));

        job.setMapperClass(MapClass.class);
        job.setNumReduceTasks(0);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(OrcOutputFormat.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(OrcStruct.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);

        return 0;
    }

    /*主函数入口*/
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new ETLJob(), args);

        System.exit(res);
    }

}




