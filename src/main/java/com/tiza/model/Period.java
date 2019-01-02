package com.tiza.model;

import lombok.Data;

/**
 * Description: Period
 * Author: DIYILIU
 * Update: 2018-09-13 14:44
 */

@Data
public class Period {

    private Long time = 0l;

    private Double mileage = .0;

    private Double fuel = .0;

    private Double cost = .0;
}
