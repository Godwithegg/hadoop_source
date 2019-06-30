package com.danhuang.mr.tablejoin;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

public class TableReducer extends Reducer<Text,TableBean,TableBean, NullWritable> {



    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Context context) throws IOException, InterruptedException {

        //存储所有订单集合
        ArrayList<TableBean> orderBeans = new ArrayList<>();
        //存储产品信息
        TableBean pdBean = new TableBean();
        for (TableBean value : values) {
            if(value.getFlag().equals("order")){//订单表
                TableBean tempBean = new TableBean();
                try {
                    //value只是一个引用，因此要建立一个新对象，才能将该对象添加到队列中
                    //如果不这样做的话，会导致只有最后一个数据能够添加到集合中
                    BeanUtils.copyProperties(tempBean,value);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
                orderBeans.add(tempBean);
            }else{
                try {
                    BeanUtils.copyProperties(pdBean,value);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
            }
        }

        for(TableBean tableBean : orderBeans){
            tableBean.setPname(pdBean.getPname());
            context.write(tableBean,NullWritable.get());
        }
    }
}
