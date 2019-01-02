package com.tiza.dao.bean;

import lombok.Data;

/**
 * Description: Vehicle
 * Author: DIYILIU
 * Update: 2018-06-28 11:34
 */

@Data
public class Vehicle {

    private Long id;

    private Double fuelPrice;

    private Integer fuelCapacity;

    private Integer enFuelLevel;

    /** 记录行程开关(0停用;1启用) **/
    private Integer enableJourney;
}
