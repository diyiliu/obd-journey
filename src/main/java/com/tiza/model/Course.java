package com.tiza.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.tiza.util.CommonUtil;
import com.tiza.util.DateUtil;
import lombok.Data;

import java.util.Date;
import java.util.Map;

/**
 * Description: Course
 * 行程
 * Author: DIYILIU
 * Update: 2018-07-20 15:49
 */

@Data
public class Course {

    private Integer vehicleId;

    private Long begin;

    private Double beginLat;

    private Double beginLng;

    private Long end;

    private Double endLat;

    private Double endLng;

    private Double mileage;

    private Double fuel;

    /**
     * 急加速
     */
    private Integer acc = 0;

    /**
     * 急减速
     */
    private Integer dec = 0;

    /**
     * 急转弯
     */
    private Integer turn = 0;

    /**
     * 急刹车
     */
    private Integer brake = 0;

    /**
     * 参考油费
     */
    private Double cost;

    private Double maxAirOpen = null;

    private Integer maxSpeed = null;

    private Integer maxRpm = null;

    private Integer maxWaterTemp = null;

    private Integer minWaterTemp = null;

    /**
     * 最大加速度
     **/
    private Double maxAcc = null;

    /**
     * 最大减速度
     **/
    private Double maxDec = null;

    private String beginAddress;

    private String endAddress;


    @JsonIgnore
    private Map beginData;

    @JsonIgnore
    private Map endData;

    /**
     * 前一条数据
     **/
    @JsonIgnore
    private Map temp;

    public void setTemp(Map temp) {
        this.temp = temp;

        int cmd = (int) temp.get("cmdId");
        if (cmd == 0x01 || cmd == 0x97){

            this.workTemp = temp;
        }
    }

    /**
     * 前一条工况数据(包括 0x97 )
     **/
    @JsonIgnore
    private Map workTemp;

    /**
     * >= 60
     **/
    private Period highSpeed = new Period();

    /**
     * >= 5 & < 60
     **/
    private Period lowSpeed = new Period();

    /**
     * < 5
     **/
    private Period idleSpeed = new Period();

    public void complete() {
        Map data1 = beginData;
        Map data2 = endData;

        double mileage1 = (double) data1.get("totalMiles");
        double consum1 = (double) data1.get("totalConsum");
        double latitude1 = (double) data1.get("latitude");
        double longitude1 = (double) data1.get("longitude");
        String timeStr1 = (String) data1.get("dateTimeStr");
        Date date1 = DateUtil.stringToDate(timeStr1);

        double mileage2 = (double) data2.get("totalMiles");
        double consum2 = (double) data2.get("totalConsum");
        double latitude2 = (double) data2.get("latitude");
        double longitude2 = (double) data2.get("longitude");
        String timeStr2 = (String) data2.get("dateTimeStr");
        Date date2 = DateUtil.stringToDate(timeStr2);

        setBegin(date1.getTime());
        setBeginLat(latitude1);
        setBeginLng(longitude1);

        setEnd(date2.getTime());
        setEndLat(latitude2);
        setEndLng(longitude2);

        setMileage(CommonUtil.keepDecimal(mileage2 - mileage1, 1));
        setFuel(CommonUtil.keepDecimal(consum2 - consum1, 2));
    }

    /**
     * 计算燃油花费
     *
     * @param price
     */
    public void calcFuelCost(double price) {
        setCost(CommonUtil.keepDecimal(fuel * price, 2));

        calcPeriodCost(highSpeed, price);
        calcPeriodCost(lowSpeed, price);
        calcPeriodCost(idleSpeed, price);
    }

    private void calcPeriodCost(Period period, double price) {

        period.setCost(CommonUtil.keepDecimal(period.getFuel() * price, 2));
    }
}
