package com.alibaba.dt.guider.storm.target.fates;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.dt.fates.common.EntityStore;
import com.alibaba.dt.fates.common.FatesCommonsLocator;
import com.alibaba.dt.fates.common.FatesDataOperator;
import com.alibaba.dt.fates.common.dto.StoreFieldValue;
import com.alibaba.dt.guider.storm.producer.MsgElement;
import com.alibaba.dt.guider.storm.storage.Storage;
import com.alibaba.dt.guider.storm.storage.hbase.HTableInstance;
import com.alibaba.dt.guider.storm.target.fates.MapValueComparator;

public class FatesInstance {

    private static final Logger LOGGER = LoggerFactory.getLogger(FatesInstance.class);

    private FatesDataOperator   fatDatOpe;
    private EntityStore         entSto;    
    private Storage             storage;
    
    public FatesInstance(HTableInstance htiForScan, 
                         HTableInstance htiForQueryItemIdToCateId, 
                         HTableInstance htiForQueryCateIdToUpperCateId, 
                         FatesDataOperator dataOperator) {
        this.fatDatOpe = dataOperator;
        this.storage = new Storage(null, htiForQueryItemIdToCateId, htiForScan, htiForQueryCateIdToUpperCateId);
    } 
    
    @SuppressWarnings({ "rawtypes", "unchecked" }) 
    public static Map sortByValue(Map unsortedMap) {
        Map sortedMap = new TreeMap(new MapValueComparator(unsortedMap));
        sortedMap.putAll(unsortedMap);
        return sortedMap;
    } 
    
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public static Map getFirstK(Map sortedMap, int k) {        
        if(sortedMap.size() > k)
            k = sortedMap.size();
        
        Map sortedFirstKMap = new LinkedHashMap();        
        for(Object key : sortedMap.keySet()) {
            if(k > 0) {
                sortedFirstKMap.put(key, sortedMap.get(key));
                k--;
            } else {
                break;
            }                
        }
        return sortedFirstKMap;
    }
    
    ///这里主要用于更新"Latest100Item"，类型为Map<String, Long>
    @SuppressWarnings({ "unchecked"})
    public Map<String, Long> addMapFieldUseOldValue(String field, Map<String, Long> realtimeMap){
        if(entSto != null){               
            StoreFieldValue stoFieVal = entSto.getFieldValue(field);
            if(stoFieVal != null) {            
                Map<String, Long> oldMap = entSto.getFieldValue(field).getMap();//用fates中历史数据          
                if(oldMap != null) {
                    realtimeMap.putAll(oldMap);//realtimeMap<String, Long>中内容为 <itemId, timestamp>
                }    
            }
            //以上是可以从fates中拿到oldMap的情况，拿到oldMap后，就把oldMap中数据都放到realtimeMap中，
            //接下来只处理realtimeMap        
            realtimeMap = getFirstK(sortByValue(realtimeMap), 100); 
            entSto.putFieldValue(field, new StoreFieldValue().newMap(realtimeMap));
            return realtimeMap;
        }
        return null;
    }

    ///这里主要用于更新"categoryTop5"，类型为Map<Long, Long>
    public void addMapFieldDisuseOldValue(String field, Map<Long, Long> map){
        if(entSto != null){  
            entSto.putFieldValue(field, new StoreFieldValue().newMap(map));//不用fates中历史数据
        }
    }    
    
    ///这里主要用于更新:
    //"CategoryWithGaussianRolloffForLatest100Item"
    //"LV1CategoryWithGaussianRolloffForLatest100Item"
    //"LV2CategoryWithGaussianRolloffForLatest100Item"，类型均为Map<Long,Double>
    public void addMapFieldDisuseOldValue(String field, List<Long> realtimeList){      
        Map<Long, Double> res = new HashMap<Long, Double>();
        
        int maxCount = 100;
        int g1=1, g2=0, g3=80;
        Gaussian gau = new Gaussian(maxCount, g1, g2, g3);        
        
        int index = 0;
        for(Long cateId : realtimeList){
            index++;
            
            double newVal = gau.g(index);
            if(res.containsKey(cateId)){
                newVal += res.get(cateId);
            }
            res.put(cateId, newVal);
            
            if(index >= maxCount){
                break;
            }
        }        
        
        if(entSto != null){  
            entSto.putFieldValue(field, new StoreFieldValue().newMap(res));//不用fates中历史数据
        }
    }    
    
    
    ///这里主要用于更新:
    //"CategoryForLatest100Item"
    //"LV1CategoryForLatest100Item"
    //"LV2CategoryForLatest100Item"，类型均为List<Long>
    public void addListFieldDisuseOldValue(String field, List<Long> realtimeList){
        if(entSto != null){  
            entSto.putFieldValue(field, new StoreFieldValue().newList(realtimeList));//不用fates中历史数据
        }
    }    
    
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void updateFates(List<FatesMapElement> fatesMapEles) {
        String subject = null;
        String field = null;
        String memSeq = null;
        boolean useOldValue = true;
        
        try {
            //把Map类型的数据，更新到Fates中
            for(FatesMapElement fatesMapEle : fatesMapEles){                
                subject = fatesMapEle.getSubject();
                field = fatesMapEle.getField();
                memSeq = String.valueOf(fatesMapEle.getMemberSeq());
                useOldValue = fatesMapEle.isUseOldValue();
                                    
                if(useOldValue == true) { //"Latest100Item"
                    HashMap<String, Long> map = (HashMap<String, Long>)fatesMapEle.getMap();
                    LinkedHashMap<String, Long> res = (LinkedHashMap<String, Long>) addMapFieldUseOldValue(field, map);
             
                    LOGGER.debug(String.format("[putMapsAndLists][Latest100Item]"));
                    ////////////////////////////////////////////////////////////////////////
                    //由最近100个商品->最近100个商品的叶子类目的List
                    //由最近100个商品->最近100个商品的一级类目的List
                    //由最近100个商品->最近100个商品的二级类目的List
                    List<Long> cateIdList = new ArrayList<Long>();
                    List<Long> lv1CateIdList = new ArrayList<Long>();
                    List<Long> lv2CateIdList = new ArrayList<Long>();
                    
                    for(String itemId : res.keySet()) {
                        //itemId->cateId
                        Long cateId = storage.itemIdToCateId(itemId);
                        cateIdList.add(cateId);
                        

                        //cateId->lv1CateId & lv2CateId
                        HashMap<String, Long> upperCateIds = 
                                storage.cateIdToUpperCateIdFromCacheOrHBase(cateId);
                        
                        for(String key: upperCateIds.keySet()){
                            Long val = upperCateIds.get(key);
                            LOGGER.debug(String.format("[cateIdToUpperCateIdFromCacheOrHBase][key:%s][val:%d]",
                                    key, val));
                        }
                        
                        lv1CateIdList.add(upperCateIds.get("cate_lv1_id"));
                        lv2CateIdList.add(upperCateIds.get("cate_lv2_id"));                          
                    }

                    addListFieldDisuseOldValue("CategoryForLatest100Item", cateIdList);
                    addListFieldDisuseOldValue("LV1CategoryForLatest100Item", lv1CateIdList);
                    addListFieldDisuseOldValue("LV2CategoryForLatest100Item", lv2CateIdList); 
                    LOGGER.debug(String.format("[putMapsAndLists][cateIdList.size:%d]"
                            + "[lv1CateIdList.size:%d][lv2CateIdList.size:%d]", 
                            cateIdList.size(), 
                            lv1CateIdList.size(),
                            lv2CateIdList.size()));

                    //////////////////////////////////////////////////////////////////////////////
                    //最近100个商品的叶子类目的List + Gaussian衰减 -> put Map<Long, Double> to Fates
                    //最近100个商品的一级类目的List + Gaussian衰减 -> put Map<Long, Double> to Fates
                    //最近100个商品的二级类目的List + Gaussian衰减 -> put Map<Long, Double> to Fates
                    addMapFieldDisuseOldValue("CategoryWithGaussianRolloffForLatest100Item", cateIdList);
                    addMapFieldDisuseOldValue("LV1CategoryWithGaussianRolloffForLatest100Item", lv1CateIdList);
                    addMapFieldDisuseOldValue("LV2CategoryWithGaussianRolloffForLatest100Item", lv2CateIdList);
                    
                } else {                  //"categoryTop5"
                    HashMap<Long, Long> map = (HashMap<Long, Long>)fatesMapEle.getMap();
                    addMapFieldDisuseOldValue(field, map);
                }                
            }            
            
            fatDatOpe.updateEntityData(subject, memSeq, entSto);
        } catch (Throwable t) {
            LOGGER.warn(String.format("[insertIntoFates fail][memSeq: %s][t:%s]",
                        memSeq, t.toString()));
        }   
    }
    
    @SuppressWarnings("rawtypes")
    public FatesMapElement categoryTop5ToFates(Long memSeq) {
        Map<Long, Long> mapForCategoryTop5 = storage.categoryTop5(memSeq);
        return new FatesMapElement<Long, Long>(memSeq, "ae_buyer", "categoryTop5", mapForCategoryTop5, false);
    }
      
    @SuppressWarnings("rawtypes")
    public FatesMapElement latest100ItemToFates(Long memSeq, ArrayList<MsgElement> msgEles) {
        ////Put mapForLatest100Item to Fates
        Map<String, Long> mapForLatest100Item = new HashMap<String, Long>();
        if(msgEles != null){
            for(MsgElement msgEle : msgEles){
                if(msgEle.getCountDelta() == 1){//为+1时要更新Latest100Item;为-1时不更新Latest100Item
                    mapForLatest100Item.put(msgEle.getItemId(), msgEle.getTimestamp());
                }
            }
        }
        if(mapForLatest100Item.size()>1)
            LOGGER.debug(String.format("[mapForLatest100Item.size:%d]", mapForLatest100Item.size()));
        return new FatesMapElement<String, Long>(memSeq, "ae_buyer", "Latest100Item", mapForLatest100Item, true);
    }
    
    @SuppressWarnings("rawtypes")
    public void dataToFates(Long memSeq, ArrayList<MsgElement> msgEles) {
        FatesMapElement fatesEleForCategoryTop5 = categoryTop5ToFates(memSeq);
        FatesMapElement fatesEleForLatest100Item = latest100ItemToFates(memSeq, msgEles);
        
        List<FatesMapElement> fatesMapEles = new ArrayList<FatesMapElement>();
        //30min类目偏好数量可增加可减少，可为0
        if(fatesEleForCategoryTop5 != null)
            fatesMapEles.add(fatesEleForCategoryTop5);
        //最近100个商品数量递增最多为100        
        if(fatesEleForLatest100Item != null && fatesEleForLatest100Item.getMap().size()>0) {
            fatesMapEles.add(fatesEleForLatest100Item);
        }
                
        entSto = fatDatOpe.queryEntityData("ae_buyer", String.valueOf(memSeq));
        updateFates(fatesMapEles);
    }

    @SuppressWarnings("rawtypes")
    public static void main(String[] args) {      
        long memSeq = 123456781L;
               
        Map<String, Long> latest100ItemMap = new HashMap<String, Long>();
        latest100ItemMap.put("1125", 115L);
        latest100ItemMap.put("1111", 101L);
        latest100ItemMap.put("1112", 102L);
        latest100ItemMap.put("1113", 103L);
        latest100ItemMap.put("1114", 104L);
        latest100ItemMap.put("1115", 105L);
        latest100ItemMap.put("1116", 106L);
        latest100ItemMap.put("1117", 107L);
        latest100ItemMap.put("1118", 108L);
        latest100ItemMap.put("1119", 109L);
        latest100ItemMap.put("1124", 114L);       
      
        Map<Long, Long> categoryTop5Map = new HashMap<Long, Long>();
        categoryTop5Map.put(17L,  18L);
        categoryTop5Map.put(27L,  28L);        
        
        FatesMapElement fatesEleForCategoryTop5 = new FatesMapElement<Long, Long>(memSeq, "ae_buyer", "categoryTop5", categoryTop5Map, false);
        FatesMapElement fatesEleForLatest100Item = new FatesMapElement<String, Long>(memSeq, "ae_buyer", "Latest100Item", latest100ItemMap, true);
        
        List< FatesMapElement > fatesEleList = new ArrayList< FatesMapElement >();
        //30min类目偏好数量可增加可减少，可为0
        if(fatesEleForCategoryTop5 != null)
            fatesEleList.add(fatesEleForCategoryTop5);
        //最近100个商品数量递增最多为100        
        if(fatesEleForLatest100Item != null && fatesEleForLatest100Item.getMap().size()>0)
            fatesEleList.add(fatesEleForLatest100Item);
      
        FatesDataOperator fatesDataOp = FatesCommonsLocator.getDataOperator();
        FatesInstance fin = new FatesInstance(null, null, null, fatesDataOp);
        
        
        fin.entSto = fin.fatDatOpe.queryEntityData("ae_buyer", String.valueOf(memSeq));
        fin.updateFates(fatesEleList);
        
        EntityStore store3 = fatesDataOp.queryEntityData("ae_buyer", String.valueOf(memSeq));
        System.out.println("[Get]" + store3.getFieldValue("Latest100Item"));
        System.out.println("[Get]" + store3.getFieldValue("categoryTop5"));

        System.out.println("exit");
        System.exit(0);
    }

}
