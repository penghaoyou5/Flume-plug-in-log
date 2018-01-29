package Flume;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.apache.log4j.Logger;
//import utils.DateUtils;
//import utils.PhitJsonUtils;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public class phitCollectFilter implements Interceptor {

    private static Logger logger = Logger.getLogger(phitCollectFilter.class);

    public phitCollectFilter() {

    }

    @Override
    public void initialize() {
        // NO-OP...
    }

    @Override
    public void close() {
        // NO-OP...
    }

    public Event intercept(Event event) {
        return null;
    }

    @Override
    public List<Event> intercept(List<Event> events) {
        List<Event> phitEvents = new ArrayList<Event>();
        List<Event> intercepted = Lists.newArrayListWithCapacity(events.size());
        for (Event event : events){
            String body = "";
            try {
                body = new String(event.getBody(), Charsets.UTF_8);

                String message;
                if (body.startsWith("{")){
                    message = body.split("\"message\":\"")[1].split("\",")[0].replace("\\","");
                }else {
                    message = body;
                }
//                System.out.println(message);
                //进行时间解析精确到天和小时

                String[] messAttr = message.split("\\s+");
                String timeStr = messAttr[0].replace(".","");
//                System.out.println(timeStr);
                long timeInt = Long.valueOf(timeStr);
//                System.out.println(timeInt);
                String date_hour = new java.text.SimpleDateFormat("yyyyMMddHH").format(new java.util.Date(timeInt));
//                System.out.println(date);

                String domainStr = messAttr[6].split("[/]+")[1].split(":")[0];
//                System.out.println(domainStr);




                Map<String, String> header = event.getHeaders();
                header.put("date_hour", date_hour); //2018-01-25-16
                header.put("domainStr", domainStr); //detect.novacdn.com

                event.setBody(message.getBytes(Charsets.UTF_8) );

                phitEvents.add(event);
            }catch(Exception e){
                logger.error("phitCollectFilter Exception error_body.["+body+"],"+e.getMessage());
            }
        }

        logger.info("phit--list event="+events.size()+",phitEvents="+phitEvents.size()+",region size=");//+regions.size());

        intercepted.addAll(phitEvents);

        return intercepted;
    }

    public static class Builder implements Interceptor.Builder {
        //使用Builder初始化Interceptor
        @Override
        public Interceptor build() {
            return new phitCollectFilter();
        }

        @Override
        public void configure(Context context) {

        }
    }

    /**
     * 主要实现时间解析和域名解析 以及分析出原始日志
     * @param args
     */
    public static void main(String[] args) {



        String origin = "{\"message\":\"1516867270.417      0 175.6.10.158 TCP_MEM_HIT/200 401 GET http://detect.novacdn.com/site_media/image/nav_bg.png  - NONE/- image/png \\\"-\\\" \\\"ChinaCache\\\" \\\"-\\\"\",\"@version\":\"1\",\"@timestamp\":\"2018-01-25T08:01:01.177Z\",\"type\":\"fc_access\",\"file\":\"/data/proclog/log/squid/access.log\",\"host\":\"CHN-HH-c-3gI\",\"offset\":\"21704386\"}";
        String message = origin.split("\"message\":\"")[1].split("\",")[0].replace("\\","");
        System.out.println(message);
        //进行时间解析精确到天和小时

        String[] messAttr = message.split("\\s+");
        String timeStr = messAttr[0].replace(".","");
        System.out.println(timeStr);
        long timeInt = Long.valueOf(timeStr);
        System.out.println(timeInt);
        String date = new java.text.SimpleDateFormat("yyyy-MM-dd-HH").format(new java.util.Date(timeInt));
        System.out.println(date);

        String domainStr = messAttr[6].split("[/]+")[1];
        System.out.println(domainStr);

    }
}
