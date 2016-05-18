package com.creek.storm.consumer.thread;

import java.util.ArrayList;
import java.util.Set;

import org.codehaus.plexus.util.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.creek.storm.producer.Bolt;
import com.creek.storm.producer.MsgElement;
import com.creek.storm.storage.Storage;
import com.creek.storm.target.fates.FatesInstance;

public class Task implements Runnable {

    private static final Logger LOGGER   = LoggerFactory.getLogger(Task.class);

    private int                 taskId;
    private Set<Long>           activeUserSet;
    private FatesInstance       fatesIns;
    private float               ratioThreshold;
    private long                lastTime = 0L;

    public Task(int tid, Set<Long> aus, FatesInstance ins, float ratioThreshold){
        this.taskId = tid;
        this.activeUserSet = aus;
        this.fatesIns = ins;
        this.ratioThreshold = ratioThreshold;
    }

    @Override
    public void run() {
        while (true) {
            Long tmpMemSeq = null;
            int curActiveUserNum = activeUserSet.size();// 当前activeUserSet中元素个数
            double ratio = curActiveUserNum / (double) Bolt.LinkedHashSetInitialCapacity;// ratio表示对activeUserSet的利用率

            try {
                synchronized (activeUserSet) {
                    while ((curActiveUserNum == 0) || ((ratio < ratioThreshold) && (taskId != 0))) {
                        // 1、当没有activeUser的时候，所有consumer线程都要阻塞wait
                        // 2、当有activeUser的时候，如果activeUserSet利用率小于0.25时，
                        // 除了0号consumer线程之外的其他线程，要阻塞wait
                        // 3、这样保证0号consumer线程一直运行，同时当0号线程处理不过来导致activeUserSet堆积时，
                        // 如果对activeUserSet的利用率ratio超过0.25后，其他consumer线程也参与进来帮着处理
                        activeUserSet.wait();

                        curActiveUserNum = activeUserSet.size();
                        ratio = curActiveUserNum / (double) Bolt.LinkedHashSetInitialCapacity;
                    }

                    // 间隔每超过3分钟打印一次ratio
                    long curTime = System.currentTimeMillis();
                    if (curTime - lastTime > Bolt.TIME_3_MIN_IN_MS) {
                        LOGGER.warn(String.format("[Task: %d][Ratio: %f][activeUserSet: %s]", taskId, ratio,
                                                  activeUserSet.toString()));
                        lastTime = curTime;
                    }

                    // 在LinkedHashSet中有数据时，
                    // 如果ratio>=0.25时，则所有的consumer线程都可以消费LinkedHashSet中数据；
                    // 如果ratio<0.25时，则只有taskId为0的consumer线程可消费数据，
                    // 其他的consumer线程接下来要转成阻塞状态
                    if ((curActiveUserNum > 0) && ((ratio >= ratioThreshold) || (taskId == 0))) {
                        tmpMemSeq = activeUserSet.iterator().next();
                        activeUserSet.remove(tmpMemSeq);
                    }
                }

                if (tmpMemSeq > 0) {
                    ArrayList<MsgElement> tmpMsgEles = Storage.pickMemSeqToMsgElements(tmpMemSeq);
                    fatesIns.dataToFates(tmpMemSeq, tmpMsgEles);
                }
            } catch (Throwable t) {// catch runtime exception
                LOGGER.error(String.format("[Catch Thread Throwable Error][TaskId: %d][activeUserSet %s][tmpId %d][Throwable %s]",
                                           taskId, activeUserSet.toString(), tmpMemSeq, t));
                LOGGER.error(ExceptionUtils.getFullStackTrace(t));
            }
        }
    }

}
