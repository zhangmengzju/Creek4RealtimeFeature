package com.alibaba.dt.guider.storm.producer;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TimeZone;

import org.apache.hadoop.hbase.client.HTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.dt.fates.common.FatesCommonsLocator;
import com.alibaba.dt.guider.storm.consumer.thread.Task;
import com.alibaba.dt.guider.storm.source.metaq.DelayMessageProducer;
import com.alibaba.dt.guider.storm.storage.Storage;
import com.alibaba.dt.guider.storm.storage.hbase.HTableInstance;
import com.alibaba.dt.guider.storm.storage.hbase.HTablePoolManager;
import com.alibaba.dt.guider.storm.target.fates.FatesInstance;
import com.alibaba.dt.guider.storm.utils.DateUtils;
import com.alibaba.dt.guider.storm.utils.MD5Utils;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.taobao.metaq.client.MetaProducer;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class Bolt implements IRichBolt {
    //static静态成员为在storm的每个worker中有个实例，该worker下的多个bolt可以共用改静态成员
    //普通成员在storm的每个worker中的每个bolt中有个实例
    private static final long        serialVersionUID             = 1L;

    public static final int          TIME_3_MIN_IN_MS             = 3 * 60 * 1000;
    public static final int          TIME_30_MIN_IN_MS            = 30 * 60 * 1000;
    public static final int          TIME_60_MIN_IN_MS            = 60 * 60 * 1000;

    public static final int          LinkedHashSetInitialCapacity = 1000;

    //////ActiveUsers
    //每个bolt(里面有5个线程)有个共用的活跃用户列表，而非多个bolt共用一个worker的活跃用户列表
    //这里将LinkedHashSet转化为线程安全的容器SynchronizedSet来使用。
    //但是需要注意的是，SynchronizedSet中add、remove、size等常用操作，
    //都有利用synchronized关键词做并发控制，所以这些操作在自己代码中可不加synchronized修饰以提高并发度。
    //但SynchronizedSet中的iterator并未利用synchronize做并发控制，
    //所以用其中的iterator进行遍历的时候要自己添加并发控制。
    private Set<Long>      activeUserSet                = 
            Collections.synchronizedSet( new LinkedHashSet<Long>(LinkedHashSetInitialCapacity));

    
    //////Storm
    private int                      taskId;
    private OutputCollector          _collector;

    //////Log
    private static final Logger      LOGGER                       = LoggerFactory
                                                                          .getLogger(Bolt.class);
    //////HBase 
    private static HTablePoolManager htpmMemSeq                   = null;                               //"ae_member_id_to_seq"
    private static HTablePoolManager htpmCateId                   = null;                               //"dim_ae_itm"
    private static HTablePoolManager htpmGuider                   = null;                               //"guider_u_realtime_feature"
 
    //////MetaQ
    private MetaProducer             producer                     = null;
    private static String            topic                        = "ae_beacon_pageview_delay_30min";

    //////Fates
    private FatesInstance            fatesIns                     = null;

    //////Storage 
    private Storage                  storage                      = null;
            
    //////Count
    private HashSet<String>          memIdUniqSet                 = new HashSet<String>();
    private long                     memIdTotCount                = 0L;

    //////ConsumerThreadNum
    private int                      consumerThreadNum            = 5;
    private float                    ratioThreshold               = 0.25f;    
    
    //////
    private Properties               properties                   = null;
    
    public Bolt(Properties properties, int consumerThreadNum, float ratioThreshold) {
        this.properties = properties;
        this.consumerThreadNum = consumerThreadNum;
        this.ratioThreshold = ratioThreshold;
    }

    public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context,
                        OutputCollector collector) {
        //////HBase
        htpmMemSeq = HTablePoolManager.getInstance((String) properties
                .get("MEMBER_ID_TO_MEMBER_SEQ_HBASE_CONF_FILE_NAME"));
        htpmCateId = HTablePoolManager.getInstance((String) properties
                .get("PRODUCT_ID_TO_CATEGORY_ID_HBASE_CONF_FILE_NAME"));
        htpmGuider = HTablePoolManager.getInstance((String) properties
                .get("GUIDER_REALTIME_HBASE_CONF_FILE_NAME"));
          
        HTableInstance htiMemIdToMemSeq = new HTableInstance((HTable) htpmMemSeq.getPool().getTable(
                (String) properties.get("MEMBER_ID_TO_MEMBER_SEQ_HBASE_TABLE_NAME")));
        HTableInstance htiItemIdToCateId = new HTableInstance((HTable) htpmCateId.getPool().getTable(
                (String) properties.get("PRODUCT_ID_TO_CATEGORY_ID_HBASE_TABLE_NAME")));
        HTableInstance htiGuider = new HTableInstance((HTable) htpmGuider.getPool().getTable(
                (String) properties.get("GUIDER_REALTIME_HBASE_TABLE_NAME")));
        HTableInstance htiCateIdToUpperCateId = new HTableInstance((HTable) htpmCateId.getPool().getTable(
                (String) properties.get("CATEGORY_ID_TO_UPPER_CATEGORY_ID_HBASE_TABLE_NAME"))); 
        LOGGER.warn(String.format("[HTablePoolManager instance member] %s", htpmMemSeq.toString()));
        LOGGER.warn(String.format("[HTablePoolManager instance category] %s", htpmCateId.toString()));
        LOGGER.warn(String.format("[HTablePoolManager instance guider] %s", htpmGuider.toString()));
        
        storage = new Storage(htiMemIdToMemSeq, htiItemIdToCateId, htiGuider, htiCateIdToUpperCateId);
        LOGGER.warn(String.format("[storage] %s", storage.toString()));        

        //////发MetaQ延时30min的消息
        this.taskId = context.getThisTaskId();
        topic = (String) properties.get("METAQ_DELAY_TOPIC");
        producer = DelayMessageProducer.init(topic, taskId);

        //////Fates
        fatesIns = new FatesInstance(htiGuider, htiItemIdToCateId, htiCateIdToUpperCateId,
                FatesCommonsLocator.getDataOperator());
        LOGGER.warn(String.format("[Fates instance] %s", fatesIns.toString()));

        //////每个bolt启动consumerThreadNum个消费activeUser的线程
        ArrayList<Thread> threads = new ArrayList<Thread>();
        for (int i = 0; i < consumerThreadNum; i++) {
            threads.add(new Thread(new Task(i, activeUserSet, fatesIns, ratioThreshold)));
        }
        for (int i = 0; i < threads.size(); i++) {
            threads.get(i).start();
        }

        _collector = collector;
    }

    public void updateHBaseCategoryCountData(long memSeq, long cateId, int countDelta) {
        String rowKey = null;
        try {
            rowKey = MD5Utils.md5(memSeq + "").substring(0, 4) + String.valueOf(memSeq)
                    + String.valueOf(cateId);
        } catch (NoSuchAlgorithmException e) {
            LOGGER.error("MD5 string error.", e);
            return;
        }

        ////写入HBase
        storage.updateHBaseCategoryCountData(rowKey, "cf", "category_count", countDelta);
    }

    public void updateFatesUserRealtimeFeature(long memSeq, String itemId, int countDelta, long timestamp, long cateId) {
        LOGGER.debug(String.format("[activeUser.size before add %d]", activeUserSet.size()));
        
        //MemSeqToMsgElements为ConcurrentHashMap,线程安全
        MsgElement msgEle = new MsgElement(memSeq, itemId, countDelta, timestamp, cateId);
        Storage.insertMemSeqToMsgElements(memSeq, msgEle);
            
        if ((activeUserSet.size() <= 1000) && (activeUserSet.contains(memSeq) == false)) {
            activeUserSet.add(memSeq);//SynchronizedSet中除iterator外的方法都是线程安全的
            synchronized (activeUserSet) {      
                activeUserSet.notifyAll();
            }
        }
        LOGGER.debug(String.format("[activeUser.size after add %d]", activeUserSet.size()));
    }

    public void sendDelayMessage(long deltaTime, String timestamp, String memberId, long memberSeq,
                                 String productId, long categoryId) {
        if (producer == null) {
            LOGGER.error("[MetaQ sendDelayMessage producer isNull]");
            return;
        }

        //收到消息后，延时30min，利用Metaq发出一个减一的msg
        //延时消息应该在(timestamp+30min)时刻发出，而非(curTime+30min)后发出
        long delayMillSec = TIME_30_MIN_IN_MS - deltaTime;
        int delayLevel = DelayMessageProducer.getDelayLevel(delayMillSec);

        //延时30min消息的body中包含"member_id,product_id,gmt_log,reduce_one"
        String body = memberId + "\005" + productId + "\005" + timestamp + "\005"
                + String.valueOf("-1");
        String tag = "metaq_delay_tag";
        String key = String.valueOf(memberSeq) + String.valueOf(categoryId);

        SendResult sendResult = null;
        try {
            sendResult = DelayMessageProducer.sendDelayMsg(producer, topic, tag, key, body,
                    delayLevel);
            LOGGER.debug(String.format("[MetaQ sendDelayMessage sendResult %s][delayLevel：%d]",
                    sendResult.toString(), delayLevel));
        } catch (Exception e) {
            if (sendResult == null)
                LOGGER.error(String.format(
                        "[MetaQ sendDelayMessage sendResult is Null][delayLevel: %d]", delayLevel),
                        e);
            else
                LOGGER.error("[MetaQ sendDelayMessage error]", e);
            return;
        }
    }

    public void execute(Tuple tuple) {

        ////////////////////////////////////////////////////////////////////////////////////
        //        Random random = new Random();
        //        if (Math.abs(random.nextInt()) % 997 != 1) {//7,97,997
        //            _collector.ack(tuple);
        //            return;
        //        }
        ////////////////////////////////////////////////////////////////////////////////////

        ////////////////////////////////////////////////////////////////////////////////////
        //        //activeUserSet [729915992, 734529074, 716815818, 735823546, 
        //        //738570750, 736155705, 142711241, 135522613, 167750919, 732283142, 
        //        //737562662, 200201381, 174261110, 702260214, 720739796, 739664241, 162881618]
        //        long memberSeq = 729915992L;
        //        
        //        ////Put map to Fates
        //        Map<Long, Long> map = new HashMap<Long, Long>();
        //        map.put(111L, 5L);
        //        map.put(222L, 4L);
        //        map.put(444L, 2L);         
        //
        //        fatesIns.putMap(map, "ae_buyer", "categoryTop5", String.valueOf(memberSeq));
        //        
        //        _collector.ack(tuple);
        //        return; 
        ////////////////////////////////////////////////////////////////////////////////////

        try {
            //member_id,product_id,timestamp,reduce_one
            //tuple有三个元素时，该条消息为用户浏览的叶子类目计数加一的消息
            //tuple有四个元素时，该条消息为延时30min的计数减一的消息，第四个元素代表减一
            LOGGER.debug(String.format("[tuple] %s", tuple.toString()));
            if ((tuple.size() != 3) && (tuple.size() != 4)) {
                LOGGER.error(String.format("[tuple] %s", tuple.toString()));
                _collector.ack(tuple);
                return;
            }

            //////timestamp  
            long t1 = System.currentTimeMillis();
            String tsStr = tuple.getString(2);
            long tsLong = 0L;
            try {
                tsLong = DateUtils.DateToLong("yyyy-MM-dd HH:mm:ss", tsStr,
                        TimeZone.getTimeZone("America/Los_Angeles"));
            } catch (ParseException e) {
                LOGGER.error("[timestamp parse error]", e);
                _collector.ack(tuple);
                return;
            }
            //////deltaTime
            //此处将近1min内活跃用户的ID保存下来，并交给定时任务去处理
            //中国时间比美国夏令时早了15个小时
            //中国时间比美国冬令时早了16个小时
            //解决方法：deltaTime=( System.currentTimeMillis() )-(timestamp.setTimeZone(LosAngles).transToGMT)

            long curTime = System.currentTimeMillis();//各个时区的机器执行该语句的结果都是相对于格林威治1970年的毫秒数，没有时区的差异
            long deltaTime = curTime - tsLong;
            LOGGER.debug(String.format("[DeltaTime %s]", deltaTime));
            //+1消息延期30min时,就不处理该消息;-1消息延时60min时，就不处理该消息

            if ((deltaTime > TIME_30_MIN_IN_MS && tuple.size() == 3)
                    || (deltaTime > TIME_60_MIN_IN_MS && tuple.size() == 4) || deltaTime < 0) {
                LOGGER.warn(String.format("[DeltaTime Wrong %s]", deltaTime));
                _collector.ack(tuple);
                return;
            }
            long t2 = System.currentTimeMillis();
            LOGGER.debug(String.format("[ cost ][timestamp %s ms]", String.valueOf(t2 - t1)));

            //////memberId和productId的异常处理
            String memId = tuple.getString(0);
            String itemId = tuple.getString(1);
            if (memId == null || memId.equals("") || memId.equals("-911")
                    || memId.equals("-") || itemId == null || itemId.equals("-911")) {
                _collector.ack(tuple);
                return;
            }

            //////memberId->memberSeq
            Long memSeq = storage.memIdToMemSeq(memId);
            if (memSeq == null || memSeq <= 0L) {
                LOGGER.error(String.format("[MemberSeq Wrong %d]", memSeq));
                _collector.ack(tuple);
                return;
            }

            //统计被有效处理的memId
            memIdUniqSet.add(memId);
            memIdTotCount++;
            LOGGER.debug(String.format("[ memId ][ memIdUniq: %s][memIdTot: %s]",
                    String.valueOf(memIdUniqSet.size()), String.valueOf(memIdTotCount)));

            //////itemId->cateId
            Long cateId = storage.itemIdToCateId(itemId);
            if (cateId == null || cateId <= 0L) {
                LOGGER.error(String.format("[CategoryId Wrong %d]", cateId));
                _collector.ack(tuple);
                return;
            }

            //////countDelta
            int countDelta = (tuple.size() == 4) ? -1 : 1;//减一消息为-1,加一消息为1

            //////////+1消息和-1消息都要更新HBase中的中间结果统计表
            long t7 = System.currentTimeMillis();
            updateHBaseCategoryCountData(memSeq, cateId, countDelta);
            long t8 = System.currentTimeMillis();
            LOGGER.debug(String.format("[ cost ][updateHBaseCategoryCountData %s ms]",
                    String.valueOf(t8 - t7)));

            //////+1消息和-1消息都要用HBase中数据经计算后更新Fates
            
            long t9 = System.currentTimeMillis();
            updateFatesUserRealtimeFeature(memSeq, itemId, countDelta, tsLong, cateId);
            long t10 = System.currentTimeMillis();
            LOGGER.debug(String.format("[ cost ][updateFatesUserRealtimeFeature %s ms]",
                    String.valueOf(t10 - t9)));

            //////////+1消息要发送延时30min的消息
            if (tuple.size() == 3) {

                //////发延时30min的代表减一的MetaQ消息
                long t11 = System.currentTimeMillis();
                //curTime = curDate.getTime();//由于和上次curTime之间有其他操作，这里重新取curTime以保证准确性
                //deltaTime = curTime - timestampLong;
                //LOGGER.warn(String.format("[sendDelayMessage deltaTime]%s", deltaTime.toString()));
                sendDelayMessage(deltaTime, tsStr, memId, memSeq, itemId,
                        cateId);
                long t12 = System.currentTimeMillis();
                LOGGER.debug(String.format("[ cost ][sendDelayMessage %s ms]",
                        String.valueOf(t12 - t11)));
            }
        } catch (Throwable t) {
            //If there's any error during execution (including prepare) Storm will kill and restart that worker
            
            StringWriter sw = new StringWriter();
            t.printStackTrace(new PrintWriter(sw));
            String fullInfo = sw.toString();
            
            if (tuple != null)
                LOGGER.error(String.format("[Catch Throwable Error][tuple:%s][error:%s]", tuple.toString(), fullInfo));
            else
                LOGGER.error("[Catch Throwable Error][tuple is null]");
            _collector.ack(tuple);
            return;
        }
        _collector.ack(tuple);
    }
    
    public void cleanup() {
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public Map getComponentConfiguration() {
        return null;
    }
}
