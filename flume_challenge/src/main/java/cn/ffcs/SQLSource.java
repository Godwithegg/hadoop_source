package cn.ffcs;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 自定义source的核心类
 */
public class SQLSource extends AbstractSource implements Configurable, PollableSource {

    /**
     * 所封装的操作类
     */
    private SQLSourceHelper sqlSourceHelper;

    @Override
    public long getBackOffSleepIncrement() {
        return 0;
    }

    @Override
    public long getMaxBackOffSleepInterval() {
        return 0;
    }


    /**
     * 初始化配置文件
     * @param context
     */
    @Override
    public void configure(Context context) {
        //初始化sqlSourceHelper
        try {
            sqlSourceHelper = new SQLSourceHelper(context);
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }


    /**
     * 不断执行相应程序：获取数据，封装event，写入channel
     * @return 返回执行状态
     * @throws EventDeliveryException
     */
    @Override
    public Status process() throws EventDeliveryException {
        try {
            //1.获取数据
            List<List<Object>> result = sqlSourceHelper.executeQuery();

            //存放事件的集合
            ArrayList<Event> events = new ArrayList<>();

            if (!result.isEmpty()) {
                //将结果数据转化为String
                List<String> allRows = sqlSourceHelper.getAllRows(result);
                //2.封装event
                SimpleEvent event;
                AtomicInteger submitCount = new AtomicInteger(0);
                for (String row : allRows) {
                    event = new SimpleEvent();
                    event.setBody(row.getBytes());
                    event.setHeaders(new HashMap<>());
                    events.add(event);
                    submitCount.getAndAdd(1);
                    //写入channel,设置最大提交数目
                    if(submitCount.get() % sqlSourceHelper.getMaxSubmitCount() == 0){
                        getChannelProcessor().processEventBatch(events);
                        events.clear();
                    }
                }
                getChannelProcessor().processEventBatch(events);


            }
            //更新元数据表
            sqlSourceHelper.updateOffset2DB();
            Thread.sleep(sqlSourceHelper.getRunQueryDelay());

            return Status.READY;
        } catch (InterruptedException e) {
            e.printStackTrace();
            return Status.BACKOFF;
        }
    }

}

