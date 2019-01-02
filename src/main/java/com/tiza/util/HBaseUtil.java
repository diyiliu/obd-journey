package com.tiza.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Description: HBaseUtil
 * Author: DIYILIU
 * Update: 2018-07-23 09:58
 */
public class HBaseUtil {
    public static final byte[] FAMILY = Bytes.toBytes("j");
    public static final byte[] QUALIFIER_V = Bytes.toBytes("v");
    private static final byte[] QUALIFIER_HIDE = Bytes.toBytes("hide");

    private Configuration config;
    private String table;

    public void put(int vehicleId, long time, String value, int hidden) throws Exception {
        try (Connection connection = ConnectionFactory.createConnection(config)) {
            TableName tableName = TableName.valueOf(table);
            Table table = connection.getTable(tableName);

            byte[] bytes = Bytes.add(Bytes.toBytes(vehicleId), Bytes.toBytes(time));
            Put p = new Put(bytes);

            p.addColumn(FAMILY, QUALIFIER_V, Bytes.toBytes(value));
            if (hidden == 0){
                p.addColumn(FAMILY, QUALIFIER_HIDE, Bytes.toBytes(true));
            }

            table.put(p);
        }
    }

    public void setConfig(Configuration config) {
        this.config = config;
    }

    public void setTable(String table) {
        this.table = table;
    }
}
