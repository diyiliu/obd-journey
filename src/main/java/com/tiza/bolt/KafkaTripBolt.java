package com.tiza.bolt;

import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.tiza.dao.VehicleDao;
import com.tiza.util.DateUtil;
import com.tiza.util.JacksonUtil;
import lombok.extern.slf4j.Slf4j;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Description: KafkaTripBolt
 * Author: DIYILIU
 * Update: 2018-07-16 14:28
 */

@Slf4j
public class KafkaTripBolt extends BaseRichBolt {

    private OutputCollector collector;

    private Long startTime;

    private Long vehicleId;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        VehicleDao.initVehicleCache();

        if (map.containsKey("startTime")) {
            startTime = (Long) map.get("startTime");
        }
        if (map.containsKey("vehicleId")) {
            vehicleId = (Long) map.get("vehicleId");
        }
    }

    @Override
    public void execute(Tuple tuple) {
        collector.ack(tuple);
        // 刷新车辆缓存
        if (tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)) {
            VehicleDao.initVehicleCache();

            return;
        }

        try {
            String kafkaMsg = tuple.getString(0);
            Map map = JacksonUtil.toObject(kafkaMsg, HashMap.class);
            if (!map.containsKey("vehicleId") || !map.containsKey("dateTimeStr")) {

                return;
            }

            Date dataTime = DateUtil.stringToDate((String) map.get("dateTimeStr"));
            if (startTime != null) {
                if (dataTime.before(new Date(startTime))) {

                    return;
                }
            }else{
                // 数据延时
                if (System.currentTimeMillis() - dataTime.getTime() > 10 * 60 * 1000){
                    log.warn("延时数据: [{}]", kafkaMsg);
                }
            }

            int id = (int) map.get("vehicleId");
            if (vehicleId == null) {
                collector.emit(new Values(id, map));
            }
            // 过滤车辆ID
            else if (vehicleId == id) {
                collector.emit(new Values(id, map));
            }
        } catch (Exception e) {
            log.info("数据源错误: [{}]", JacksonUtil.toJson(tuple));
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

        outputFieldsDeclarer.declare(new Fields("vehicleId", "data"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> map = new HashMap();
        //给当前bolt设置定时任务 5分钟
        map.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 300);

        return map;
    }
}
