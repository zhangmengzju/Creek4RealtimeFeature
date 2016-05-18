package com.creek.storm.source.metaq;

/**
 * Copyright (C) 2010-2013 Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.taobao.metaq.client.MetaPushConsumer;

import java.util.List;

public class OldPushConsumer {

    /**
     * 当前例子是PushConsumer用法，使用方式给用户感觉是消息从MetaQ服务器推到了应用客户端。<br>
     * 但是实际PushConsumer内部是使用长轮询Pull方式从MetaQ服务器拉消息，然后再回调用户Listener方法<br>
     */
    public static void main(String[] args) throws InterruptedException, MQClientException {
        /**
         * 一个应用创建一个Consumer，由应用来维护此对象，可以设置为全局对象或者单例<br>
         * 注意：ConsumerGroupName需要由应用来保证唯一<br>
         * ConsumerGroupName在生产环境需要申请，非生产环境不需要
         */
        MetaPushConsumer consumer = new MetaPushConsumer("RebalanceTest_Consumer_Group");

        /**
         * 订阅指定topic下tags分别等于TagA或TagC或TagD
         */
        consumer.subscribe("TopicTest4", "TagA || TagC || TagD");
        consumer.setConsumeMessageBatchMaxSize(3);

        //consumer.subscribe("ae_beacon_pageview", "*");
        //consumer.setConsumeMessageBatchMaxSize(3);

        /**
         * 订阅指定topic下所有消息<br>
         * 注意：一个consumer对象可以订阅多个topic
         */
        consumer.subscribe("TopicTest3", "*");

        consumer.registerMessageListener(new MessageListenerConcurrently() {

            /**
             * 1、默认msgs里只有一条消息，可以通过设置consumeMessageBatchMaxSize参数来批量接收消息<br>
             * 2、如果设置为批量消费方式，要么都成功，要么都失败。<br>
             * 3、此方法由MetaQ客户端多个线程回调，需要应用来处理并发安全问题<br>
             * 4、抛异常与返回ConsumeConcurrentlyStatus.RECONSUME_LATER等价<br>
             * 5、每条消息失败后，会尝试重试，重试5次都失败，则丢弃<br>
             */
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                                                            ConsumeConcurrentlyContext context) {
                System.out.println(Thread.currentThread().getName() + " Receive New Messages: "
                        + msgs);
                for (MessageExt msg : msgs) {
                    if (msg.getTags().equals("TagA")) {
                        return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                    }
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        /**
         * Consumer对象在使用之前必须要调用start初始化，初始化一次即可<br>
         */
        consumer.start();

        System.out.println("Consumer Started.");
    }
}
