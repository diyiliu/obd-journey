package com.tiza.dao;

import com.tiza.dao.bean.Vehicle;
import com.tiza.util.JdbcUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;

/**
 * Description: VehicleDao
 * Author: DIYILIU
 * Update: 2018-07-23 17:15
 */

public class VehicleDao {
    public static Map<String, Vehicle> vehicleCache = new HashMap();

    /**
     * 实时油位
     *
     * @param id
     * @return
     */
    public static double actualFuelLevel(long id) {
        String sql = "SELECT t.systemfuellevel fuellevel " +
                " FROM vehicle t" +
                " WHERE t.id=" + id;

        double level = 0;

        JdbcUtil jdbcUtil = new JdbcUtil();
        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        try {
            connection = jdbcUtil.getConnection();
            statement = connection.prepareStatement(sql);
            rs = statement.executeQuery();
            while (rs.next()) {
                level = rs.getDouble("fuellevel");
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            jdbcUtil.closeAll(rs, statement, connection);
        }

        return level;
    }

    public static void initVehicleCache() {
        String sql = "SELECT t.id," +
                "       f.price," +
                "       t.fuelcapacity," +
                "       t.enablesystemfuellevel enfuellevel," +
                "       t.enablejourney enablejourney" +
                "  FROM vehicle t" +
                "  LEFT JOIN fuel_type f" +
                "    ON f.id = t.fueltypeid";

        JdbcUtil jdbcUtil = new JdbcUtil();

        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        try {
            connection = jdbcUtil.getConnection();
            statement = connection.prepareStatement(sql);
            rs = statement.executeQuery();
            while (rs.next()) {
                long id = rs.getLong("id");
                double price = rs.getDouble("price");

                int fuelCapacity = rs.getInt("fuelcapacity");
                int enFuelLevel = rs.getInt("enfuellevel");
                int enableJourney = rs.getInt("enablejourney");

                Vehicle vehicle = new Vehicle();
                vehicle.setId(id);
                vehicle.setFuelPrice(price);
                vehicle.setEnableJourney(enableJourney);

                if (enFuelLevel == 1 && fuelCapacity > 0) {
                    vehicle.setEnFuelLevel(1);
                    vehicle.setFuelCapacity(fuelCapacity);
                } else {
                    vehicle.setEnFuelLevel(0);
                }

                vehicleCache.put(String.valueOf(id), vehicle);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            jdbcUtil.closeAll(rs, statement, connection);
        }
    }

}
