package com.alibaba.dt.guider.storm.source.metaq;

import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.remoting.exception.RemotingException;
import com.taobao.metaq.client.MetaProducer;

public class DelayMessageProducer {

    private static final int TIME_1_SEC_IN_MS = 1 * 1000;
    private static final int TIME_5_SEC_IN_MS = 5 * 1000;
    private static final int TIME_10_SEC_IN_MS = 10 * 1000;
    private static final int TIME_30_SEC_IN_MS = 30 * 1000;
    private static final int TIME_1_MIN_IN_MS = 1 * 60 * 1000;
    private static final int TIME_2_MIN_IN_MS = 2 * 60 * 1000;
    private static final int TIME_3_MIN_IN_MS = 3 * 60 * 1000;
    private static final int TIME_4_MIN_IN_MS = 4 * 60 * 1000;
    private static final int TIME_5_MIN_IN_MS = 5 * 60 * 1000;
    private static final int TIME_6_MIN_IN_MS = 6 * 60 * 1000;
    private static final int TIME_7_MIN_IN_MS = 7 * 60 * 1000;
    private static final int TIME_8_MIN_IN_MS = 8 * 60 * 1000;
    private static final int TIME_9_MIN_IN_MS = 9 * 60 * 1000;
    private static final int TIME_10_MIN_IN_MS = 10 * 60 * 1000;
    private static final int TIME_20_MIN_IN_MS = 20 * 60 * 1000;
    private static final int TIME_30_MIN_IN_MS = 30 * 60 * 1000;
    
    public static MetaProducer init(String topic, int taskId) {
        MetaProducer producer = new MetaProducer(topic + taskId); //"manhongTestPubGroup"
        /**
         * Producer对象在使用之前必须要调用start初始化，初始化一次即可
         * 注意：切记不可以在每次发送消息时，都调用start方法
         */
        try {
            producer.start();
        } catch (MQClientException e) {
            e.printStackTrace();
        }
        return producer;
    }

    public static void shutdown(MetaProducer producer) {
        /**
         * 应用退出时，要调用shutdown来清理资源，关闭网络连接，从MetaQ服务器上注销自己
         * 注意：我们建议应用在JBOSS、Tomcat等容器的退出钩子里调用shutdown方法
         */
        producer.shutdown();
    }

    public static SendResult sendDelayMsg(MetaProducer producer, String topic, String tag,
                                          String key, String body, int delayLevel)
            throws MQClientException, RemotingException, MQBrokerException, InterruptedException {

        /**
         * 下面这段代码表明一个Producer对象可以发送多个topic，多个tag的消息。
         * 注意：send方法是同步调用，只要不抛异常就标识成功。但是发送成功也可会有多种状态，<br>
         * 例如消息写入Master成功，但是Slave不成功，这种情况消息属于成功，但是对于个别应用如果对消息可靠性要求极高，<br>
         * 需要对这种情况做处理。另外，消息可能会存在发送失败的情况，失败重试由应用来处理。
         */
        try {
            Message msg = new Message(topic,// topic "ae_beacon_pageview_delay_30min"
                    tag,                    //"TagA"
                    key,                    //"OrderID001"，消息的Key字段是为了唯一标识消息的，方便运维排查问题。如果不设置Key，则无法定位消息丢失原因。
                    body.getBytes());       // body

            //////////////////////////////////
            //设置延时级别为16，代表该msg会在30min后发出
            if (delayLevel >= 1 && delayLevel <= 16)
                msg.setDelayTimeLevel(delayLevel);
            //////////////////////////////////

            SendResult sendResult = producer.send(msg);
            return sendResult;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }    
    
    public static int getDelayLevel(long delayMillSec) {
        if (delayMillSec > TIME_1_SEC_IN_MS && delayMillSec <= TIME_5_SEC_IN_MS)//需要延时1s到5s的，延时1s
            return 1;
        else if (delayMillSec > TIME_5_SEC_IN_MS && delayMillSec <= TIME_10_SEC_IN_MS)//需要延时5s到10s的，延时5s
            return 2;
        else if (delayMillSec > TIME_10_SEC_IN_MS && delayMillSec <= TIME_30_SEC_IN_MS)//需要延时10s到30s的，延时10s
            return 3;
        else if (delayMillSec > TIME_30_SEC_IN_MS && delayMillSec <= TIME_1_MIN_IN_MS)//需要延时30s到1min的，延时30s
            return 4;
        else if (delayMillSec > TIME_1_MIN_IN_MS && delayMillSec <= TIME_2_MIN_IN_MS)//需要延时1min到2min的，延时1min
            return 5;
        else if (delayMillSec > TIME_2_MIN_IN_MS && delayMillSec <= TIME_3_MIN_IN_MS)//需要延时2min到3min的，延时2min
            return 6;
        else if (delayMillSec > TIME_3_MIN_IN_MS && delayMillSec <= TIME_4_MIN_IN_MS)//需要延时3min到4min的，延时3min
            return 7;
        else if (delayMillSec > TIME_4_MIN_IN_MS && delayMillSec <= TIME_5_MIN_IN_MS)//需要延时4min到5min的，延时4min
            return 8;
        else if (delayMillSec > TIME_5_MIN_IN_MS && delayMillSec <= TIME_6_MIN_IN_MS)//需要延时5min到6min的，延时5min
            return 9;
        else if (delayMillSec > TIME_6_MIN_IN_MS && delayMillSec <= TIME_7_MIN_IN_MS)//需要延时6min到7min的，延时6min
            return 10;
        else if (delayMillSec > TIME_7_MIN_IN_MS && delayMillSec <= TIME_8_MIN_IN_MS)//需要延时7min到8min的，延时7min
            return 11;
        else if (delayMillSec > TIME_8_MIN_IN_MS && delayMillSec <= TIME_9_MIN_IN_MS)//需要延时8min到9min的，延时8min
            return 12;
        else if (delayMillSec > TIME_9_MIN_IN_MS && delayMillSec <= TIME_10_MIN_IN_MS)//需要延时9min到10min的，延时9min
            return 13;
        else if (delayMillSec > TIME_10_MIN_IN_MS && delayMillSec <= TIME_20_MIN_IN_MS)//需要延时10min到20min的，延时10min
            return 14;
        else if (delayMillSec > TIME_20_MIN_IN_MS && delayMillSec <= TIME_30_MIN_IN_MS)//需要延时20min到30min的，延时20min
            return 15;
        else
            return 0;
    }

    public static void main(String[] args) throws MQClientException, RemotingException,
            MQBrokerException, InterruptedException {

        String topic = "ae_beacon_pageview_delay_30min";
        String tag = "TagA";
        String key = "OrderID001";
        String body = "user_22334" + "\005" + "product_id" + "\005" + "gmt_1255" + "\005" + "-1";
        int delayLevel = 0;

        MetaProducer producer = init(topic, 123);
        for (int i = 0; i < 20; i++) {
            SendResult sendResult = sendDelayMsg(producer, topic, tag, key, body, delayLevel);
            System.out.println("[sendResult=" + i + "]" + sendResult);
        }
        System.out.println("[end]");
    }
}
