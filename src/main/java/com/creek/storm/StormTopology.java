package com.creek.storm;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import com.alibaba.da.plough.spout.MetaQPushSpout;
import com.alibaba.da.plough.spout.PloughSpoutFactory;
import com.alibaba.da.plough.spout.TimeTunnelConfig;
import com.alibaba.da.plough.spout.TimeTunnelSpout;
import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.remoting.exception.RemotingException;
import com.creek.storm.producer.Bolt;
import com.creek.storm.utils.StreamFileUtils;

public class StormTopology {

    private static final Logger LOGGER = LoggerFactory.getLogger(StormTopology.class);

    public static void receiveMsg(int workerNum, int ackerNum, int ttPCSpoutNum, int ttAPPSpoutNum,
                                  int metaqDelaySpoutNum, int boltNum, int maxSpoutPending, int consumerThreadNum,
                                  String country, float ratioThreshold) throws AlreadyAliveException,
                                                                       InvalidTopologyException {
        // //////////////////////////////////////////////////
        // project config
        Properties properties = new Properties();
        try {
            properties.load(StormTopology.class.getResourceAsStream("/" + country.toLowerCase()
                                                                    + "-creek-storm.properties"));
        } catch (IOException e) {
            throw new RuntimeException("Project properties file load error.", e);// 用RuntimeException去中断程序运行
        }

        // ////////////////////////////////////////
        // TT: PC端
        // {country}-tt-api-rule-pc.properties
        // 读TT配置文件:inputStream to tempFilePC
        File tempFilePC = null;
        try {
            tempFilePC = StreamFileUtils.stream2file(StormTopology.class.getResourceAsStream("/"
                                                                                             + country.toLowerCase()
                                                                                             + "-tt-api-rule-pc.properties"));
        } catch (IOException e) {
            throw new RuntimeException("TT pc properties file load error.", e);// 用RuntimeException去中断程序运行
        }
        TimeTunnelConfig ttApiRuleConfigPC = TimeTunnelConfig.loadConfigFromFile(tempFilePC.getPath());
        ttApiRuleConfigPC.setFieldSplit("\005");

        // ////////////////////////////////////////
        // TT: APP端
        // {country}-tt-api-rule-app.properties
        // 读TT配置文件:inputStream to tempFileAPP
        File tempFileAPP = null;
        try {
            tempFileAPP = StreamFileUtils.stream2file(StormTopology.class.getResourceAsStream("/"
                                                                                              + country.toLowerCase()
                                                                                              + "-tt-api-rule-app.properties"));
        } catch (IOException e) {
            throw new RuntimeException("TT app properties file load error.", e);// 用RuntimeException去中断程序运行
        }
        TimeTunnelConfig ttApiRuleConfigAPP = TimeTunnelConfig.loadConfigFromFile(tempFileAPP.getPath());
        ttApiRuleConfigAPP.setFieldSplit("\001");

        // /////////////////////////////////////////////
        // TimeTunnel -> Spout_1
        TimeTunnelSpout ttsPC = PloughSpoutFactory.createTimeTunnelSpout(ttApiRuleConfigPC);

        // /////////////////////////////////////////////
        // TimeTunnel -> Spout_2
        TimeTunnelSpout ttsAPP = PloughSpoutFactory.createTimeTunnelSpout(ttApiRuleConfigAPP);

        // /////////////////////////////////////////////
        // MetaQ -> Spout_3
        // 延时的MetaQ数据，收到后对应memberSeq_categoryId的count减一
        MetaQPushSpout mqDelay = new MetaQPushSpout("ae_beacon_pageview_delay_30min", "guider_metaq_group",
                                                    "member_id,product_id,gmt_log,reduce_one");

        // /////////////////////////////////////////////
        // Topology
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("tt_spout_pc", ttsPC, ttPCSpoutNum);
        builder.setSpout("tt_spout_app", ttsAPP, ttAPPSpoutNum);
        builder.setSpout("metaq_delay_spout", mqDelay, metaqDelaySpoutNum);

        builder.setBolt("processor", new Bolt(properties, consumerThreadNum, ratioThreshold), boltNum).fieldsGrouping("tt_spout_pc",
                                                                                                                      new Fields(
                                                                                                                                 "member_id")).fieldsGrouping("tt_spout_app",
                                                                                                                                                              new Fields(
                                                                                                                                                                         "long_login_nick"))// 确认是member_id
        .fieldsGrouping("metaq_delay_spout", new Fields("member_id"));

        // /////////////////////////////////////////////
        // Config
        Config conf = new Config();
        conf.setNumWorkers(workerNum);
        conf.setMaxSpoutPending(maxSpoutPending);// 队列长度
        conf.setMessageTimeoutSecs(60);
        conf.setNumAckers(ackerNum);
        conf.setDebug(false);
        // JStorm 安装完后，默认的NIMBUS端口配置为7672
        conf.put(Config.NIMBUS_THRIFT_PORT, 7672);
        ConfigExtension.setUserDefinedLog4jConf(conf, "udf.log4j.properties");
        StormSubmitter.submitTopology("creek_storm", conf, builder.createTopology());
        LOGGER.warn("storm cluster will start");
    }

    public static void main(String[] args) throws MQClientException, RemotingException, MQBrokerException,
                                          InterruptedException, AlreadyAliveException, InvalidTopologyException {

        if (args.length < 10) {
            LOGGER.error(String.format("args num error!"));
            for (int i = 0; i < args.length; i++) {
                LOGGER.error(String.format("[args %d ] %s", i, args[i]));
            }
            return;
        }

        int workerNum = Integer.valueOf(args[0]);// 30
        int ackerNum = Integer.valueOf(args[1]);// 3
        int ttPCSpoutNum = Integer.valueOf(args[2]);// 8
        int ttAPPSpoutNum = Integer.valueOf(args[3]);// 8
        int metaqDelaySpoutNum = Integer.valueOf(args[4]);// 8
        int boltNum = Integer.valueOf(args[5]);// 100
        int maxSpoutPending = Integer.valueOf(args[6]);// 600
        int consumerThreadNum = Integer.valueOf(args[7]);// 50
        String country = args[8];// CN
        float ratioThreshold = Float.valueOf(args[9]);// 0.25

        // 接收正常消息和延时消息
        receiveMsg(workerNum, ackerNum, ttPCSpoutNum, ttAPPSpoutNum, metaqDelaySpoutNum, boltNum, maxSpoutPending,
                   consumerThreadNum, country, ratioThreshold);
    }
}
