package com.creek.storm.storage.hbase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTablePool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.da.utils.HBaseUtils;

public class HTablePoolManager {

    //////Log
    private static final Logger      LOGGER   = LoggerFactory.getLogger(HTablePoolManager.class);

    private HTablePool        pool = null;                                                       //HTablePool，主要负责管理HTable实例，是线程安全的

    private static HashMap<String, HTablePoolManager> confNameToPoolManager = null;

    //线程安全的懒汉式单例
    public static synchronized HTablePoolManager getInstance(String configFileName) {
        if (confNameToPoolManager == null){
            confNameToPoolManager = new HashMap<String, HTablePoolManager>();
        } 
        if(confNameToPoolManager.get(configFileName) == null) {
            confNameToPoolManager.put(configFileName, new HTablePoolManager(configFileName));
        }
        return confNameToPoolManager.get(configFileName);
    }

    public HTablePoolManager(String configFileName) {

        Configuration conf;

        Properties properties = new Properties();
        try {
            properties.load(HTablePoolManager.class.getResourceAsStream("/" + configFileName
                    + ".properties"));
        } catch (IOException e) {
            LOGGER.error("[HBase properties file load error.]["+ configFileName + "]" + e);
        }

        if (configFileName.contains("diamond")) {
            conf = HBaseUtils.getConf(properties);
        } else {
            conf = HBaseConfiguration.create();
            for (Object key : properties.keySet()) {
                Object value = properties.get(key);
                conf.set((String) key, (String) value);
            }
        }

        pool = new HTablePool(conf, 100);//maxSize:maximum number of references to keep for each table
    }

    public synchronized HTablePool getPool() {
        return pool;
    }
    
    public static void main(String[] args) throws IOException {
        //        System.out.println("[begin]");
        //
        //        HTablePoolManager htiGuider = new HTablePoolManager(); //"guider_u_realtime_feature" 上海
        //        HTableInstance htGuider = htiGuider.init("guider_u_realtime_feature", "hbase-perf");
        //
        //        /////////////////////////////////////////////////////////////////////////////
        //        //Put Bytes.toBytes((Integer) value) to HBase
        //        //Get bytes from HBase
        //        //Use Bytes.toInt(bytes) to get Int value   
        //        htiGuider.put(htGuider, "row13579", "cf", "value", 123456);
        //        byte[] cateCountBefore = htiGuider.get(htGuider, "row13579", "cf", "value");
        //        System.out.println("[cateCountBefore.length]" + cateCountBefore.length);
        //        Integer cateInt = Bytes.toInt(cateCountBefore);
        //        System.out.println("[cateInt]" + cateInt);
        //
        //        /////////////////////////////////////////////////////////////////////////////
        //        //Put Bytes.toBytes((String) value) to HBase
        //        //Get bytes from HBase
        //        //Use Bytes.toString(bytes) and Integer.valueOf(String) to get Int value
        //        htiGuider.put(htGuider, "row2468", "cf", "value", "123456");
        //        byte[] cateCountBefore2 = htiGuider.get(htGuider, "row2468", "cf", "value");
        //        System.out.println("[cateCountBefore2.length]" + cateCountBefore2.length);
        //        String cateCountStr2 = Bytes.toString(cateCountBefore2);
        //        Integer cateInt2 = Integer.valueOf(cateCountStr2);
        //        System.out.println("[cateInt2]" + cateInt2);
        //
        //        htiGuider.delete(htGuider, "row2468");
        //        byte[] cateCount3 = htiGuider.get(htGuider, "row2468", "cf", "value");
        //        System.out.println("[cateCount3]" + cateCount3);
        //
        //        System.out.println("[end]");
    }
}
