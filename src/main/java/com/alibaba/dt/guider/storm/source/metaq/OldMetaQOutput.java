package com.alibaba.dt.guider.storm.source.metaq;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.UUID;

import org.apache.log4j.Logger;

import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.MessageQueueSelector;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.client.producer.SendStatus;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.remoting.exception.RemotingException;
import com.taobao.metaq.client.MetaProducer;

public class OldMetaQOutput {

    // create producer
    private MetaProducer  producer;
    // publish topic
    private String        topic_name;
    @SuppressWarnings("unused")
    private String        groupName;
    private static Logger LOG             = Logger.getLogger(OldMetaQOutput.class);

    private final int     Max_Retry_Times = 5;

    public OldMetaQOutput(String topic_name, String groupName, String instanceName) {

        try {
            this.topic_name = topic_name;
            this.groupName = groupName;
            producer = new MetaProducer(groupName);
            producer.setInstanceName(instanceName);
            producer.start();
            producer.setRetryTimesWhenSendFailed(Max_Retry_Times);
        } catch (MQClientException e) {
            LOG.error("MetaQOutput init error", e);
        }

    }

    public void output(String key, String msg) throws MQClientException, RemotingException,
            MQBrokerException, InterruptedException {

        Message message = new Message(topic_name, null, key, msg.getBytes());

        SendResult sendResult = producer.send(message, new MessageQueueSelector() {
            public MessageQueue select(List<MessageQueue> mqs, Message message, Object arg) {
                String id = (String) arg;
                int index = Math.abs(id.hashCode()) % mqs.size();
                return mqs.get(index);
            }
        }, key);
        SendStatus status = sendResult.getSendStatus();
        switch (status) {
            case SEND_OK:
                //			LOG.warn("Send message successfully,sent to "
                //					+ msg);
                break;
            default:
                LOG.error("Send message failed,error message: " + status + " msg " + msg);
        }

    }

    public static void main(String[] args) throws MQClientException, RemotingException,
            MQBrokerException, InterruptedException, IOException {
        OldMetaQOutput metaq = new OldMetaQOutput("ae_beacon_pageview", "bbbb", "d");
        FileReader f = new FileReader("C:\\Users\\sheng.yaos\\Desktop\\data.log");
        BufferedReader br = new BufferedReader(f);
        String s;
        while ((s = br.readLine()) != null) {

            //String msg = "{\"discount\":0,\"image_count\":1,\"image_version\":\"0\",\"memberSeq\":\"1122323\",\"now_max_price\":0,\"now_min_price\":0,\"now_price\":90,\"product_id\":\"6711210\",\"product_status\":\"approved\",\"product_subject\":\"auto test product\",\"product_type\":\"static\",\"product_unit\":\"sell_by_piece\",\"relation_type\":\"shopcart\",\"sku_attr\":\"11:11\",\"sku_image\":\"a\",\"sku_image_version\":\"1\",\"update_tag\":\"I\",\"view_price\":500,\"wish_max_price\":0,\"wish_min_price\":0,\"ws_offline_date\":\"2013-10-23 00:00:00.0\"}";
            metaq.output(UUID.randomUUID().toString(), s);
        }
        br.close();
    }

    public void close() {
        if (producer != null)
            producer.shutdown();
    }

}
