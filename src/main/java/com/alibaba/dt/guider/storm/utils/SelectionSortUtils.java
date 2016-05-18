package com.alibaba.dt.guider.storm.utils;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class SelectionSortUtils {

    //<key,value,timestamp>的对象
    public static class KVPair {
        public String key;
        public int    value;
        public long   timestamp;

        public KVPair(String key, int value, long timestamp) {
            this.key = key;
            this.value = value;
            this.timestamp = timestamp;
        }
    }

    //将一个HBase的结果，转化成一个<key,value,timestamp>的对象
    public static KVPair resultToKVPair(Result res) {
        KeyValue[] kvArr = res.raw();

        String key = new String(kvArr[0].getRow());
        int value = Bytes.toInt(kvArr[0].getValue());
        long timestamp = kvArr[0].getTimestamp();

        return new KVPair(key, value, timestamp);
    }

    //将一组HBase的结果，转化成一组<key,value,timestamp>的对象
    public static KVPair[] resultArrayToKVPairArray(Result[] resArr) {
        KVPair[] kvpArr = new KVPair[resArr.length];
        for (int i = 0; i < resArr.length; i++) {
            kvpArr[i] = resultToKVPair(resArr[i]);
        }
        return kvpArr;
    }

    //将一组HBase的结果，按value值选择出最大的TopK个
    public static KVPair[] selectSortTopK(Result[] resArr, int topK) {
        KVPair[] kvpArr = resultArrayToKVPairArray(resArr);

        if (kvpArr.length <= 1) {
            return kvpArr;
        }

        int position = 0;
        topK = (topK < kvpArr.length ? topK : kvpArr.length);

        for (int i = 0; i < topK; i++) {
            int j = i + 1;
            position = i;

            KVPair temp = kvpArr[i];
            for (; j < kvpArr.length; j++) {
                if (kvpArr[j].value > temp.value) {
                    temp = kvpArr[j];
                    position = j;
                }
            }

            kvpArr[position] = kvpArr[i];
            kvpArr[i] = temp;
        }
        return kvpArr;
    }

    //将一组int值，选择出最大的TopK个
    public static int[] selectSortTopN(int[] a, int topK) {
        if (a.length <= 1) {
            return a;
        }

        int position = 0;
        topK = (topK < a.length ? topK : a.length);

        for (int i = 0; i < topK; i++) {
            int j = i + 1;
            position = i;

            int temp = a[i];
            for (; j < a.length; j++) {
                if (a[j] > temp) {
                    temp = a[j];
                    position = j;
                }
            }

            a[position] = a[i];
            a[i] = temp;
        }
        return a;
    }

    public static void main(String[] args) {
        int a[] = { 1, 54, 6, 3, 78, 34, 12, 45 };
        int newArr[] = selectSortTopN(a, 4);
        for (int i = 0; i < newArr.length; i++)
            System.out.println(newArr[i]);
    }

}
