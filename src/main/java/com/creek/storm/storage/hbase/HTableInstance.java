package com.creek.storm.storage.hbase;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HTableInstance implements Serializable{ 
    private static final long serialVersionUID = 1554086538073387512L;
    
    //////Log
    private static final Logger LOGGER = LoggerFactory.getLogger(HTableInstance.class);
    private HTable              table  = null;

    public HTableInstance(HTable tableInput) {
        table = tableInput;
    }

    public void put(String rowKey, String family, String qualifier, Object value) {
        //一个PUT代表一行数据，再NEW一个PUT表示第二行数据,每行一个唯一的ROWKEY，此处rowkey为put构造方法中传入的值
        Put put = new Put(rowKey.getBytes());

        if (value instanceof Integer) {
            put.add(family.getBytes(), qualifier.getBytes(), Bytes.toBytes((Integer) value));
        } else if (value instanceof String) {
            put.add(family.getBytes(), qualifier.getBytes(), Bytes.toBytes((String) value));
        } else if (value instanceof Double) {
            put.add(family.getBytes(), qualifier.getBytes(), Bytes.toBytes((Double) value));
        } else if (value instanceof Long) {
            put.add(family.getBytes(), qualifier.getBytes(), Bytes.toBytes((Long) value));
        } else if (value instanceof Float) {
            put.add(family.getBytes(), qualifier.getBytes(), Bytes.toBytes((Float) value));
        } else {
            LOGGER.error(String.format("[unsupport data type]%s", value.getClass().getName()));
            return;
        }

        try {
            table.put(put);
            LOGGER.debug(String.format(
                    "[HBase put success][rowKey: %s][family: %s][qualifier: %s][value: %s]",
                    rowKey, family, qualifier, value));
        } catch (IOException e) {
            LOGGER.error(String.format(
                    "[HBase put fail][rowKey: %s][family: %s][qualifier: %s][value: %s]", rowKey,
                    family, qualifier, value), e);
            return;
        }
    }

    public byte[] get(String rowKey, String family, String qualifier) throws IOException {
        Get g = new Get(Bytes.toBytes(rowKey));
        g.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));

        return table.get(g).getValue(Bytes.toBytes(family), Bytes.toBytes(qualifier));
    }

    public HashMap<String, byte[]> get(String rowKey, String family, HashSet<String> colNameSet) throws IOException {
        HashMap<String, byte[]> res = new HashMap<String, byte[]>();
        
        Get g = new Get(Bytes.toBytes(rowKey));
        
        for(String colName : colNameSet){
            g.addColumn(Bytes.toBytes(family), Bytes.toBytes(colName));
        }

        Result queRes = table.get(g);
        for(KeyValue kv : queRes.raw()){
            String colStr = Bytes.toString(kv.getQualifier());
            byte[] val = kv.getValue();
            if(colNameSet.contains(colStr))
                res.put(colStr, val);
        }
        return res;
    }
    
    public void delete(String rowKey) throws IOException {
        Delete d = new Delete(Bytes.toBytes(rowKey));
        table.delete(d);
    }

    public ResultScanner scan(String prefix) {
        LOGGER.debug(String.format("[HBase][scan][table: %s][prefix: %s]", table, prefix));
        byte[] prefixKey = prefix.getBytes();

        Scan scan = new Scan();
        scan.setStartRow(prefixKey);
        scan.setStopRow(Bytes.add(prefixKey, new byte[] { (byte) 0xff }));

        ResultScanner resScanner = null;
        try {
            resScanner = table.getScanner(scan);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return resScanner;
    }

    public void updateHBaseCategoryCountData(String rowKey, String family, String qualifier,
                                             int deltaCount) {
        //此处处理收到的消息，分加一消息和减一消息两种，
        //去HBase中get对应元素
        //若不为null,则update对应记录再put
        //若为null,则直接put
        try {
            //标志HBase中是否存在该rowkey
            boolean rowkeyExistFlag = false;

            int cateCountBefore = 0;
            byte[] cateCount = get(rowKey, family, qualifier);
            if (cateCount != null) {
                rowkeyExistFlag = true;
                cateCountBefore = Bytes.toInt(cateCount);
                LOGGER.debug(String.format("[updateHBaseCategoryCountData old cateCountInt %d]",
                        cateCountBefore));
            } else {
                LOGGER.debug("[updateHBaseCategoryCountData cateCount is Null]");
            }

            int cateCountAfter = cateCountBefore + deltaCount;

            LOGGER.debug(String.format("[updateHBaseCategoryCountData deltaCount %d]", deltaCount));

            if (cateCountAfter > 0)
                put(rowKey, family, qualifier, cateCountAfter);//使用范型来进行支持Int->Bytes,String->Bytes等的转换
            else if (rowkeyExistFlag == true)
                delete(rowKey);

        } catch (IOException e) {
            LOGGER.error("HBase updateHBaseCategoryCountData error.", e);
            return;
        }
    }
}
