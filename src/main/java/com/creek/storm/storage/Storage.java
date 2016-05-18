package com.creek.storm.storage;

import java.io.IOException;
import java.io.Serializable;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.creek.storm.producer.MsgElement;
import com.creek.storm.storage.hbase.HTableInstance;
import com.creek.storm.utils.LRUCache;
import com.creek.storm.utils.MD5Utils;
import com.creek.storm.utils.SelectionSortUtils;
import com.creek.storm.utils.SelectionSortUtils.KVPair;
import com.google.common.collect.Lists;

public class Storage implements Serializable{
    private static final long serialVersionUID = -6682332067459174694L;

    //////Log
    private static final Logger      LOGGER                       = LoggerFactory
                                                                          .getLogger(Storage.class);

    public static final int          LRUCacheInitialCapacity      = 10000;
    
    //////LRUCache
    private static LRUCache<String, Long>   memIdToMemSeqCache = new LRUCache<String, Long>(
                                                                          LRUCacheInitialCapacity);
    private static LRUCache<String, Long>   itemIdToCateIdCache = new LRUCache<String, Long>(
                                                                          LRUCacheInitialCapacity);
    private static LRUCache<Long, Long>   cateIdToLV1CateIdCache = new LRUCache<Long, Long>(
                                                                          LRUCacheInitialCapacity);
    private static LRUCache<Long, Long>   cateIdToLV2CateIdCache = new LRUCache<Long, Long>(
                                                                          LRUCacheInitialCapacity);
    ////memSeq -> MsgElements
    private static ConcurrentHashMap<Long, ArrayList<MsgElement>> memSeqToMsgElements = new ConcurrentHashMap<Long, ArrayList<MsgElement>>();
    
    private HTableInstance           htiMemIdToMemSeq              = null;
    private HTableInstance           htiItemIdToCateId             = null;
    private HTableInstance           htiGuider                     = null;
    private HTableInstance           htiCateIdToUpperCateId        = null;   
   
    public Storage(HTableInstance htiMemIdToMemSeq, 
                   HTableInstance htiItemIdToCateId,
                   HTableInstance htiGuider, 
                   HTableInstance htiCateIdToUpperCateId) {
        this.htiMemIdToMemSeq = htiMemIdToMemSeq;
        this.htiItemIdToCateId = htiItemIdToCateId;
        this.htiGuider = htiGuider;
        this.htiCateIdToUpperCateId = htiCateIdToUpperCateId;
    }

    @Override
    public String toString() {
        return "Storage [htiMemIdToMemSeq=" + htiMemIdToMemSeq + ", htiItemIdToCateId="
                + htiItemIdToCateId + ", htiGuider=" + htiGuider + ", htiCateIdToUpperCateId="
                + htiCateIdToUpperCateId + "]";
    }

    
    ////memSeq -> MsgElements
    public static void insertMemSeqToMsgElements(Long memSeq, MsgElement msgEle) {
        ArrayList<MsgElement> msgEles = Storage.memSeqToMsgElements.get(memSeq);
        if(msgEles == null)
            msgEles = new ArrayList<MsgElement>();
        msgEles.add(msgEle);
        Storage.memSeqToMsgElements.put(memSeq, msgEles);
    }
   
    public static ArrayList<MsgElement> pickMemSeqToMsgElements(Long memSeq) {
        ArrayList<MsgElement> res = Storage.memSeqToMsgElements.get(memSeq);
        Storage.memSeqToMsgElements.remove(memSeq);
        return res;
    }
    
    ////memId -> memSeq
    public static synchronized LRUCache<String, Long> getMemIdToMemSeqCache() {
        return memIdToMemSeqCache;
    }
    
    public static synchronized void insertMemIdToMemSeqCache(String memId, Long MemSeq) {
        Storage.memIdToMemSeqCache.put(memId, MemSeq);
    }

    ////itemId -> cateId
    public static synchronized LRUCache<String, Long> getItemIdToCateIdCache() {
        return itemIdToCateIdCache;
    }

    public static synchronized void insertItemIdToCateIdCache(String itemId, Long cateId) {
        Storage.itemIdToCateIdCache.put(itemId, cateId);
    }
    
    ////cateId -> lv1CateId
    public static synchronized LRUCache<Long, Long> getCateIdToLV1CateIdCache() {
        return cateIdToLV1CateIdCache;
    }

    public static synchronized void insertCateIdToLV1CateIdCache(Long cateId, Long lv1CateId) {
        Storage.cateIdToLV1CateIdCache.put(cateId, lv1CateId);
    }
        
    ////cateId -> lv2CateId
    public static synchronized LRUCache<Long, Long> getCateIdToLV2CateIdCache() {
        return cateIdToLV2CateIdCache;
    }

    public static synchronized void insertCateIdToLV2CateIdCache(Long cateId, Long lv2CateId) {
        Storage.cateIdToLV2CateIdCache.put(cateId, lv2CateId);
    }       

    ////cateId -> cache -> lv1CateId & lv2CateId
    public HashMap<String, Long> cateIdToUpperCateIdFromCacheOrHBase(Long cateId) {
        HashMap<String, Long> res = new HashMap<String, Long>();
           
        Long lv1CateId = getCateIdToLV1CateIdCache().get(cateId);
        Long lv2CateId = getCateIdToLV2CateIdCache().get(cateId);
        if (lv1CateId != null && lv2CateId != null ) { 
            ///cate_lv1_id和cate_lv2_id均命中cache
            res.put("cate_lv1_id", lv1CateId);
            res.put("cate_lv2_id", lv2CateId);
            return res;
        } else if (lv1CateId == null && lv2CateId != null){
            ///cate_lv1_id未命中cache，去HBase中找
            lv1CateId = cateIdToUpperCateIdFromHBase(cateId, "cate_lv1_id");
            insertCateIdToLV1CateIdCache(cateId, lv1CateId);
            res.put("cate_lv1_id", lv1CateId);
            res.put("cate_lv2_id", lv2CateId);
            return res;
        } else if (lv1CateId != null && lv2CateId == null){
            ///cate_lv2_id未命中cache，去HBase中找
            lv2CateId = cateIdToUpperCateIdFromHBase(cateId, "cate_lv2_id");
            insertCateIdToLV2CateIdCache(cateId, lv2CateId);
            res.put("cate_lv1_id", lv1CateId);
            res.put("cate_lv2_id", lv2CateId);
            return res;
        } else {
            ///cate_lv1_id和cate_lv2_id均未命中cache，去HBase中找，找到结果后再写Cache
            HashSet<String> colNames = new HashSet<String>();
            colNames.add("cate_lv1_id");
            colNames.add("cate_lv2_id");
            return cateIdToUpperCateIdAllFromHBase(cateId, colNames);         
        } 
    }
    
    ////cateId -> hbase -> lv1CateId & lv2CateId
    public HashMap<String, Long> cateIdToUpperCateIdAllFromHBase(Long cateId, HashSet<String> colNames) {
        HashMap<String, Long> res = new HashMap<String, Long>();
        
        long t1 = System.currentTimeMillis();       
        String cateIdStr = String.valueOf(cateId);        
        
        HashMap<String, byte[]> upperCateIdBytes = null;
        try {
            upperCateIdBytes = htiCateIdToUpperCateId.get(cateIdStr, "i", colNames);
        } catch (Exception e) {
            LOGGER.error("[HBase get method error]%s", e);
            return null;
        }
        if (upperCateIdBytes == null) {
            LOGGER.error(String.format("[cateIdStr: %s][lv1CateIdBytes == null]", cateIdStr));
            return null;
        }
        Long lv1CateId = Long.valueOf(Bytes.toString(upperCateIdBytes.get("cate_lv1_id")));
        Long lv2CateId = Long.valueOf(Bytes.toString(upperCateIdBytes.get("cate_lv2_id")));
        insertCateIdToLV1CateIdCache(cateId, lv1CateId);
        insertCateIdToLV2CateIdCache(cateId, lv2CateId);
        res.put("cate_lv1_id", lv1CateId);
        res.put("cate_lv2_id", lv2CateId);
        LOGGER.debug(String.format("[cateIdStr: %s][lv1CateId: %d][lv2CateId: %d]", cateIdStr, lv1CateId, lv2CateId));
         
        long t2 = System.currentTimeMillis();
        LOGGER.debug(String.format("[cost][lv1CateId %s ms][lv1CacheRatio:%f][lv2CacheRatio:%f]",
                String.valueOf(t2 - t1), 
                getCateIdToLV1CateIdCache().size()/10000f, 
                getCateIdToLV2CateIdCache().size()/10000f));
        return res;    
    }
    
    ////cateId -> hbase -> lv1CateId or lv2CateId
    public Long cateIdToUpperCateIdFromHBase(Long cateId, String colName) {
        long t1 = System.currentTimeMillis();       
        String cateIdStr = String.valueOf(cateId);
        Long upperCateId = -1L;
        byte[] upperCateIdBytes = null;
        try {
            upperCateIdBytes = htiCateIdToUpperCateId.get(cateIdStr, "i", colName);
        } catch (Exception e) {
            LOGGER.error("[HBase get method error]%s", e);
            return null;
        }
        if (upperCateIdBytes == null) {
            LOGGER.error(String.format("[cateIdStr: %s][colName:%s][upperCateIdBytes == null]", cateIdStr, colName));
            return null;
        }
        upperCateId = Long.valueOf(Bytes.toString(upperCateIdBytes));
        LOGGER.debug(String.format("[cateIdStr: %s][colName:%s][upperCateId: %d]", cateIdStr, colName, upperCateId));
        long t2 = System.currentTimeMillis();
        LOGGER.debug(String.format("[cost][colName:%s][upperCateId %s ms][ratio:]", colName, String.valueOf(t2 - t1), getCateIdToLV1CateIdCache().size()/10000f));
        return upperCateId;
    }
    
    ////delta count from msg -> hbase
    public void updateHBaseCategoryCountData(String rowKey, String colFamily, String colName, int countDelta){
        htiGuider.updateHBaseCategoryCountData(rowKey, colFamily, colName, countDelta);
    }    
    
    ////memSeq -> scan hbase with memSeq -> Top 5 memSeq: <cateId, count>
    public Map<Long, Long> categoryTop5(long memSeq) { 
        String prefix = null;
        try {
            prefix = MD5Utils.md5(String.valueOf(memSeq)).substring(0, 4) + String.valueOf(memSeq);
        } catch (NoSuchAlgorithmException e) {
            LOGGER.error(String.format("[MD5 error]%s", e));
        }

        Iterator<Result> rsIter = htiGuider.scan(prefix).iterator();
        
        if (rsIter != null && rsIter.hasNext() != false) {
            //KVPair3个成员变量
            //key:fe4e3545240223702464562100002861 => MD5(memberSeq).substring(0,4) + memberSeq + categoryId
            //timestamp:1441893115660
            //value:1
            List<Result> rsList = Lists.newArrayList(rsIter);
            Result[] rsArr = new Result[rsList.size()];
            rsList.toArray(rsArr);

            int topK = rsArr.length < 5 ? rsArr.length : 5;//categoryCount的前5个或者更少
            KVPair[] kvTop5 = SelectionSortUtils.selectSortTopK(rsArr, topK);

            ////Put mapForCategoryTop5 to Fates
            Map<Long, Long> mapForCategoryTop5 = new HashMap<Long, Long>();
            for (int i = 0; i < topK; i++) {
                long cateId = Long.valueOf(kvTop5[i].key.replaceFirst(prefix, ""));
                long cateCount = kvTop5[i].value;
                mapForCategoryTop5.put(cateId, cateCount);
            }            
            
            return mapForCategoryTop5;
        } else {
            LOGGER.debug(String.format("[scan hbase but get nothing][memSeq: %d]", memSeq));
            //scan了HBase,但是HBase中没有结果时，要往"categoryTop5"中put进去一个null
            return new HashMap<Long, Long>();
        }
    }
    
    ////itemId -> cache or hbase -> cateId
    public Long itemIdToCateId(String itemId) {
        long t1 = System.currentTimeMillis();
        Long cateId = getItemIdToCateIdCache().get(itemId);
        if (cateId == null) {
            //HBase以Long_to_Bytes的方式存categoryId,
            //所以要得到Long型的categoryId,只要Bytes_to_Long即可
            //如下：
            //categoryId = Bytes.toLong(categoryIdBytes)
            byte[] categoryIdBytes = null;
            try {
                categoryIdBytes = htiItemIdToCateId.get(MD5Utils.md5(itemId), "i", "category_id");
            } catch (Exception e) {
                LOGGER.error("HBase get(MD5(Stirng)) method error.", e);
                return null;
            }
            if (categoryIdBytes == null) {
                LOGGER.error(String.format("[itemId: %s][cateIdBytes == null]", itemId));
                return null;
            }
            cateId = Bytes.toLong(categoryIdBytes);
            insertItemIdToCateIdCache(itemId, cateId);
        } else {
            LOGGER.debug(String.format("[ cache ][proId: %s][cateId: %s]", itemId,
                    String.valueOf(cateId)));
        }
        long t2 = System.currentTimeMillis();
        LOGGER.debug(String.format("[ cost ][categoryId %s ms]", String.valueOf(t2 - t1)));
        return cateId;
    }

    ////memId -> cache or hbase -> memSeq
    public Long memIdToMemSeq(String memId) {
        //////memberId -> memberSeq
        long t3 = System.currentTimeMillis();
        Long memSeq =  getMemIdToMemSeqCache().get(memId);
        if (memSeq == null) {
            //HBase以String_to_Bytes的方式存memberSeq,
            //所以要得到Long型的memberSeq,要先Bytes_to_String,然后String_to_Long
            //如下：
            //String memberSeqStr = Bytes.toString(memberSeqBytes);
            //Long memberSeq = Long.valueOf(memberSeqStr);
            byte[] memSeqBytes = null;
            try {
                memSeqBytes = htiMemIdToMemSeq.get(memId, "i", "member_seq");
            } catch (IOException e) {
                LOGGER.error("HBase get method error.", e);
                return null;
            }
            if (memSeqBytes == null) {
                LOGGER.error(String.format("[memberId: %s][memberSeqBytes == null]", memId));
                return null;
            }
            String memSeqStr = Bytes.toString(memSeqBytes);
            memSeq = Long.valueOf(memSeqStr);
            insertMemIdToMemSeqCache(memId, memSeq);
        } else {
            LOGGER.debug(String.format("[ cache ][memId: %s][memSeq: %s]", memId,
                    String.valueOf(memSeq)));
        }
        long t4 = System.currentTimeMillis();
        LOGGER.debug(String.format("[ cost ][memSeq %s ms]", String.valueOf(t4 - t3)));
        return memSeq;
    }
}
