package com.tiza.bolt;

import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.tiza.dao.VehicleDao;
import com.tiza.dao.bean.Vehicle;
import com.tiza.model.Course;
import com.tiza.model.Period;
import com.tiza.util.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.MapUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.*;

/**
 * Description: ReportTripBolt
 * Author: DIYILIU
 * Update: 2018-07-18 10:14
 */

@Slf4j
public class ReportTripBolt extends BaseRichBolt {
    private OutputCollector collector;

    private Map<String, Course> tripMap;

    /**
     * 车辆熄火缓存
     **/
    private Map<String, Date> offMap;

    private HBaseUtil hbaseUtil;

    private Configuration config;

    private String gpsTable;

    private String obdTable;

    private String amapKey;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        tripMap = new HashMap();
        offMap = new HashMap();

        config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", (String) map.get("zk-quorum"));
        config.set("hbase.zookeeper.property.clientPort", (String) map.get("zk-port"));
        String table = (String) map.get("table");

        hbaseUtil = new HBaseUtil();
        hbaseUtil.setConfig(config);
        hbaseUtil.setTable(table);

        gpsTable = (String) map.get("gps-table");
        obdTable = (String) map.get("obd-table");
        // 高德地图 应用KEY
        amapKey = (String) map.get("amap-key");
    }

    @Override
    public void execute(Tuple tuple) {
        collector.ack(tuple);

        // 检测车辆是否数据异常
        if (tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)) {
            Set<String> keySet = tripMap.keySet();
            for (Iterator<String> iterator = keySet.iterator(); iterator.hasNext(); ) {
                String key = iterator.next();
                int vehicleId = Integer.valueOf(key);

                if (checkOff(key, vehicleId, new Date())) {
                    iterator.remove();
                }
            }

            return;
        }

        int vehicleId = tuple.getIntegerByField("vehicleId");
        Map data = (Map) tuple.getValueByField("data");
        String key = String.valueOf(vehicleId);

        // 过滤时间错乱
        if (isOutDate(key, data)) {

            log.info("车辆[{}]数据时间异常: [{}]", vehicleId, JacksonUtil.toJson(data));
            return;
        }

        int cmd = (int) data.get("cmdId");
        if (0x97 == cmd) {
            int accOn = (int) data.get("accOn");
            if (accOn == 1) {
                // 结束上次 点火未结束的行程
                if (tripMap.containsKey(key)) {
/*
                    String timeStr = (String) data.get("dateTimeStr");
                    Date date = DateUtil.stringToDate(timeStr);

                    if (checkOff(key, vehicleId, date)) {
                        // 行程结束
                        tripMap.remove(key);
                    }
*/
                } else {
                    double lat = (double) data.get("latitude");
                    double lng = (double) data.get("longitude");
                    if (lat > 0 || lng > 0) {
                        Course course = new Course();
                        course.setVehicleId(vehicleId);
                        course.setBeginData(data);
                        course.setTemp(data);

                        // 点火
                        log.info("车辆[{}]点火: [{}]", vehicleId, JacksonUtil.toJson(data));
                        tripMap.put(key, course);
                        return;
                    }
                }
            }

            int accOff = (int) data.get("accOff");
            if (accOff == 1 && tripMap.containsKey(key)) {
                Course course = tripMap.get(key);
                course.setEndData(data);
                course.setTemp(data);

                // 记录熄火时间
                Date offDate = DateUtil.stringToDate((String) data.get("dateTimeStr"));
                offMap.put(key, offDate);
/*
                // 熄火
                tripMap.remove(key);
                // 里程结束
                speedPeriod(course, data);
                course.complete();
                log.info("车辆[{}]熄火: [{}]", vehicleId, JacksonUtil.toJson(course));
                toHBase(course);
*/
                return;
            }
        } else {
            if (tripMap.containsKey(key)) {
                Course course = tripMap.get(key);
                Map last = course.getTemp();
                String timeStr = (String) last.get("dateTimeStr");
                if (isOutDate(last, data)) {
                    forceOff(key, vehicleId, timeStr);
                    tripMap.remove(key);
                } else {
                    Date lastDate = DateUtil.stringToDate(timeStr);
                    Date now = DateUtil.stringToDate((String) data.get("dateTimeStr"));
                    if (now.after(lastDate)) {

                        proCourse(data, course);
                        // 合并行程
                        if (course.getEndData() != null) {
                            String endStr = (String) course.getEndData().get("dateTimeStr");
                            Date endDate = DateUtil.stringToDate(endStr);
                            if (now.after(endDate)) {
                                course.setEndData(null);
                            }
                        }
                    }
                }
            } else {
                // 工况数据
                if (0x01 == cmd) {
                    String timeStr = (String) data.get("dateTimeStr");
                    forceOn(key, vehicleId, timeStr);
                }
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> map = new HashMap();
        //给当前bolt设置定时任务 3分钟
        map.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 3 * 60);

        return map;
    }

    /**
     * 检测工况里程异常
     * (初始化里程, 行程异常)
     */
    private void checkMileage(Map map1, Map map2) {
        Date d1 = DateUtil.stringToDate((String) map1.get("dateTimeStr"));
        double m1 = getMileage(map1);

        Date d2 = DateUtil.stringToDate((String) map2.get("dateTimeStr"));
        double m2 = getMileage(map2);

        double speed = ((m2 - m1) * 1000 * 3600) / (d2.getTime() - d1.getTime());
        // 速度 > 300 (km/h)数据异常
        if (speed > 300) {
            map1.put("totalMiles", m2);
        }
    }

    private boolean isOutDate(String key, Map data) {
        Date dataTime = DateUtil.stringToDate((String) data.get("dateTimeStr"));
        if (offMap.containsKey(key)) {
            Date offTime = offMap.get(key);
            if (!dataTime.after(offTime)) {

                return true;
            }
        }

        return false;
    }

    /**
     * 数据是否过期 (10分钟)
     *
     * @param last
     * @param data
     * @return
     */
    private boolean isOutDate(Map last, Map data) {
        Date d1 = DateUtil.stringToDate((String) last.get("dateTimeStr"));
        Date d2 = DateUtil.stringToDate((String) data.get("dateTimeStr"));

        if (d2.getTime() - d1.getTime() > 10 * 60 * 1000) {

            return true;
        }

        return false;
    }


    /**
     * 十分钟间隔认为车辆熄火
     */
    private boolean checkOff(String key, int vehicleId, Date now) {
        Course course = tripMap.get(key);
        Map last = course.getTemp();
        String timeStr = (String) last.get("dateTimeStr");
        Date lastDate = DateUtil.stringToDate(timeStr);

        if (now.getTime() - lastDate.getTime() > 10 * 60 * 1000) {
            forceOff(key, vehicleId, timeStr);
            return true;
        }

        return false;
    }

    private void forceOn(String key, int vehicleId, String timeStr) {
        Date date = DateUtil.stringToDate(timeStr);
        try {
            if (date != null) {
                Map map = fetchData(vehicleId, date.getTime());
                map.put("accOn", 1);
                map.put("accOff", 0);
                map.put("cmdId", 0x97);
                map.put("dateTimeStr", map.get("dateTimeStr"));
                map.put("auto", 1);

                // 过滤时间错乱
                if (isOutDate(key, map)) {

                    log.info("车辆[{}]数据时间异常: [{}]", vehicleId, JacksonUtil.toJson(map));
                    return;
                }

                Course course = new Course();
                course.setVehicleId(vehicleId);
                course.setBeginData(map);
                course.setTemp(map);

                log.info("车辆[{}]强制点火: [{}]", vehicleId, JacksonUtil.toJson(map));
                tripMap.put(key, course);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void forceOff(String key, int vehicleId, String timeStr) {
        Date date = DateUtil.stringToDate(timeStr);
        Course course = tripMap.get(key);
        try {
            if (course.getEndData() == null && date != null) {
                Map map = fetchData(vehicleId, date.getTime());
                map.put("accOn", 0);
                map.put("accOff", 1);
                map.put("cmdId", 0x97);
                map.put("dateTimeStr", map.get("dateTimeStr"));
                map.put("auto", 1);
                course.setEndData(map);
            }

            Map map = course.getEndData();
            // 记录熄火时间
            Date offDate = DateUtil.stringToDate((String) map.get("dateTimeStr"));
            offMap.put(key, offDate);

            speedPeriod(course, map);
            course.complete();
            log.info("车辆[{}]强制熄火: [{}]", vehicleId, JacksonUtil.toJson(course));

            // 修改车辆状态
            String sql = "update VEHICLE_GPS t set t.ACCSTATUS=0, t.VEHICLESTATUS=0 where t.VEHICLEID=" + vehicleId;
            JdbcUtil jdbcUtil = new JdbcUtil();
            jdbcUtil.execute(sql);
            // 写入HBase
            toHBase(course);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void proCourse(Map data, Course course) {
        if (course == null) {

            return;
        }

        int cmd = (int) data.get("cmdId");
        /** 工况数据 */
        if (0x01 == cmd) {
            // 设置里程,行程统计异常
            Map begin = course.getBeginData();
            checkMileage(begin, data);


            // 水温
            if (data.containsKey("engineCoolantTemperature")) {
                int tem = (int) data.get("engineCoolantTemperature");
                if (course.getMaxWaterTemp() == null ||
                        tem > course.getMaxWaterTemp()) {
                    course.setMaxWaterTemp(tem);
                }
                if (course.getMinWaterTemp() == null ||
                        tem < course.getMinWaterTemp()) {
                    course.setMinWaterTemp(tem);
                }
            }

            // 转速
            if (data.containsKey("engineRPM")) {
                int rpm = (int) data.get("engineRPM");
                if (course.getMaxRpm() == null ||
                        rpm > course.getMaxRpm()) {
                    course.setMaxRpm(rpm);
                }
            }

            // 节气门
            if (data.containsKey("throttlePositionSensor")) {
                double airOpen = (double) data.get("throttlePositionSensor");
                if (course.getMaxAirOpen() == null ||
                        airOpen > course.getMaxAirOpen()) {
                    course.setMaxAirOpen(airOpen);
                }
            }

            // 速度
            if (data.containsKey("vehicleSpeed")) {
                int speed = (int) data.get("vehicleSpeed");
                if (course.getMaxSpeed() == null ||
                        speed > course.getMaxSpeed()) {
                    course.setMaxSpeed(speed);
                }
            }

            // 累计速度区间数据
            speedPeriod(course, data);
        }
        /** 急减速 */
        else if (0xA8 == cmd) {
            course.setDec(course.getDec() + 1);

            double accel = (double) data.get("reloadG");
            if (course.getMaxDec() == null ||
                    accel > course.getMaxDec()) {
                course.setMaxDec(accel);
            }
        }
        /** 急加速 */
        else if (0xA9 == cmd) {
            course.setAcc(course.getAcc() + 1);

            double accel = (double) data.get("reloadG");
            if (course.getMaxAcc() == null ||
                    accel > course.getMaxAcc()) {
                course.setMaxAcc(accel);
            }
        }
        /** 急转弯 */
        else if (0xAA == cmd) {
            course.setTurn(course.getTurn() + 1);

        }
        /** 急刹车 */
        else if (0xAC == cmd) {
            course.setBrake(course.getBrake() + 1);

            double accel = (double) data.get("reloadG");
            if (course.getMaxDec() == null ||
                    accel > course.getMaxDec()) {
                course.setMaxDec(accel);
            }
        }

        // 记录当前数据
        course.setTemp(data);
    }

    private void speedPeriod(Course course, Map data) {
        Map temp = course.getWorkTemp();
        double mileage1 = getMileage(temp);
        double consum1 = getConsum(temp);

        double mileage2 = getMileage(data);
        double consum2 = getConsum(data);

        double mileage = mileage2 - mileage1;
        double consum = consum2 - consum1;
        long time = DateUtil.stringToDate((String) data.get("dateTimeStr")).getTime() -
                DateUtil.stringToDate((String) temp.get("dateTimeStr")).getTime();

        if (mileage > 10 || consum > 1) {
            log.info("异常数据: [{}, {}]", JacksonUtil.toJson(temp), JacksonUtil.toJson(data));
        }

        // 低速
        Period period = course.getLowSpeed();
        if (data.containsKey("vehicleSpeed")) {
            int speed = (int) data.get("vehicleSpeed");
            // 高速
            if (speed >= 80) {

                period = course.getHighSpeed();
            }
            // 怠速
            else if (speed < 5) {

                period = course.getIdleSpeed();
            }
        }

        period.setMileage(CommonUtil.keepDecimal(period.getMileage() + mileage, 2));
        period.setFuel(CommonUtil.keepDecimal(period.getFuel() + consum, 3));
        period.setTime(period.getTime() + time / 1000);
    }

    private double getMileage(Map data) {

        if (data.containsKey("totalMiles")) {

            return (double) data.get("totalMiles");
        }

        if (data.containsKey("totalMileage")) {

            return (double) data.get("totalMileage");
        }

        return 0;
    }

    private double getConsum(Map data) {

        if (data.containsKey("totalConsum")) {

            return (double) data.get("totalConsum");
        }

        if (data.containsKey("totalFuelConsumption")) {

            return (double) data.get("totalFuelConsumption");
        }

        return 0;
    }

    private void toHBase(Course course) {
        // 过异常数据
        if (course.getBegin() == null || course.getEnd() == null ||
                (course.getMileage() < 0.5 && course.getFuel() < 0.05)) {
            log.info("过滤数据: [{}], 原始内容: [{}]", JacksonUtil.toJson(course));

            return;
        }
        Map<String, Vehicle> vehicleCache = VehicleDao.vehicleCache;
        String vehicleId = String.valueOf(course.getVehicleId());
        if (vehicleCache.containsKey(vehicleId)) {
            Vehicle vehicle = vehicleCache.get(vehicleId);
            Double price = vehicle.getFuelPrice();
            if (price != null) {

                course.calcFuelCost(price);
            }

            // 高德解析经纬度地址
            course.setBeginAddress(getAddress(course.getBeginLat(), course.getBeginLng()));
            course.setEndAddress(getAddress(course.getEndLat(), course.getEndLng()));

            try {
                String json = JacksonUtil.toJson(course);
                hbaseUtil.put(course.getVehicleId(), course.getBegin(), json, vehicle.getEnableJourney());
                log.info("写入HBase: [{}]", json);
            } catch (Exception e) {
                e.printStackTrace();
            }

            // 更新油耗信息
            if (vehicle.getEnFuelLevel() == 1) {
                double level = VehicleDao.actualFuelLevel(vehicle.getId());
                if (level > 0) {
                    updateFuelLevel(vehicle, level, course.getFuel());
                }
            }
        }
    }

    private void updateFuelLevel(Vehicle vehicle, double fuelLevel, double costFuel) {
        double nowFuel = vehicle.getFuelCapacity() * fuelLevel - costFuel * 100;
        double percent = nowFuel > 0 ? CommonUtil.keepDecimal(nowFuel / vehicle.getFuelCapacity(), 1) : 0;

        String sql = "update VEHICLE t set t.systemfuellevel=" + percent + " where t.id=" + vehicle.getId();
        JdbcUtil jdbcUtil = new JdbcUtil();
        jdbcUtil.execute(sql);

        log.info("修改车辆[{}]系统油位至[{}], 本次消耗油量[{}]。", vehicle.getId(), percent, costFuel);
    }


    /**
     * 获取经纬度地址(高德API)
     *
     * @param lat
     * @param lng
     * @return
     */
    private String getAddress(double lat, double lng) {
        double[] latLng = GpsCorrectUtil.gps84_To_Gcj02(lat, lng);
        try {
            Map map = GpsCorrectUtil.reverseLngLat(latLng[1], latLng[0], amapKey);

            if (MapUtils.isNotEmpty(map)) {

                return (String) map.get("address");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    private Map fetchData(int id, long time) throws Exception {
        // 获取经纬度
        double lat = 0, lng = 0;
        long t = 0;
        try (Connection connection = ConnectionFactory.createConnection(config)) {
            TableName tableName = TableName.valueOf(gpsTable);
            Table table = connection.getTable(tableName);

            byte[] family = Bytes.toBytes("j");
            byte[] start = Bytes.add(Bytes.toBytes(id), Bytes.toBytes(time));

            byte[] latColumn = Bytes.toBytes("wgs84lat");
            byte[] lngColumn = Bytes.toBytes("wgs84lng");

            Scan scan = new Scan(start);
            scan.addColumn(family, latColumn);
            scan.addColumn(family, lngColumn);

            Filter filter = new RowFilter(CompareFilter.CompareOp.LESS, new BinaryComparator(start));
            scan.setFilter(filter);
            // 倒序
            scan.setReversed(true);

            ResultScanner rs = table.getScanner(scan);
            try {
                for (Result r : rs) {
                    byte[] bytes = r.getRow();
                    int i = Bytes.toInt(bytes);
                    if (i != id) {
                        continue;
                    }
                    t = Bytes.toLong(bytes, 4);
                    if (r.containsNonEmptyColumn(family, latColumn) &&
                            r.containsNonEmptyColumn(family, lngColumn)) {

                        String latStr = Bytes.toString(r.getValue(family, latColumn));
                        String lngStr = Bytes.toString(r.getValue(family, lngColumn));
                        lat = Double.valueOf(latStr);
                        lng = Double.valueOf(lngStr);

                        if (lat > 0 || lng > 0) {
                            break;
                        }
                    }
                }
            } finally {
                rs.close();
            }
        }

        // 获取里程 油耗
        double totalMiles = 0, totalConsum = 0;
        try (Connection connection = ConnectionFactory.createConnection(config)) {
            TableName tableName = TableName.valueOf(obdTable);
            Table table = connection.getTable(tableName);

            byte[] family = Bytes.toBytes("j");
            byte[] start = Bytes.add(Bytes.toBytes(id), Bytes.toBytes(time));

            byte[] mileColumn = Bytes.toBytes("obdMileage");
            byte[] fuelColumn = Bytes.toBytes("obdFuelConsumption");

            Scan scan = new Scan(start);
            scan.addColumn(family, mileColumn);
            scan.addColumn(family, fuelColumn);

            Filter filter = new RowFilter(CompareFilter.CompareOp.LESS_OR_EQUAL, new BinaryComparator(start));
            scan.setFilter(filter);
            scan.setReversed(true);

            ResultScanner rs = table.getScanner(scan);
            try {
                for (Result r : rs) {
                    byte[] bytes = r.getRow();
                    int i = Bytes.toInt(bytes);

                    if (i != id) {
                        continue;
                    }

                    if (r.containsNonEmptyColumn(family, mileColumn) &&
                            r.containsNonEmptyColumn(family, fuelColumn)) {

                        String mileStr = Bytes.toString(r.getValue(family, mileColumn));
                        String consumStr = Bytes.toString(r.getValue(family, fuelColumn));
                        totalMiles = Double.valueOf(mileStr);
                        totalConsum = Double.valueOf(consumStr);

                        if (totalMiles > 0 && totalConsum > 0) {
                            break;
                        }
                    }
                }
            } finally {
                rs.close();
            }
        }

        Map map = new HashMap();
        map.put("totalMiles", totalMiles);
        map.put("totalConsum", totalConsum);
        map.put("latitude", lat);
        map.put("longitude", lng);
        map.put("dateTimeStr", DateUtil.dateToString(new Date(t)));

        return map;
    }
}
