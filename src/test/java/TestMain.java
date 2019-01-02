import com.tiza.util.DateUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.util.Calendar;
import java.util.Date;

/**
 * Description: TestMain
 * Author: DIYILIU
 * Update: 2018-08-07 10:53
 */


public class TestMain {

    @Test
    public void test() {

        System.out.println(20 - 30.1/60);

    }


    @Test
    public void testHBase() throws Exception {
        org.apache.hadoop.conf.Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "xg153,xg154,xg155");
        config.set("hbase.zookeeper.property.clientPort", "2181");

        try (Connection connection = ConnectionFactory.createConnection(config)) {
            TableName tableName = TableName.valueOf("obd:vehicle_gps");
            Table table = connection.getTable(tableName);

            byte[] family = Bytes.toBytes("j");

/*
            Calendar calendar = Calendar.getInstance();
            calendar.add(Calendar.DAY_OF_MONTH, -10);
*/

            long time = System.currentTimeMillis();
            byte[] start = Bytes.add(Bytes.toBytes(1), Bytes.toBytes(time));

            Scan scan = new Scan(start);
            scan.addFamily(family);

            Filter filter = new RowFilter(CompareFilter.CompareOp.LESS_OR_EQUAL, new BinaryComparator(start));
            scan.setFilter(filter);
            scan.setReversed(true);

            ResultScanner rs = table.getScanner(scan);
            try {
                int i = 0;
                for (Result r = rs.next(); r != null; r = rs.next()) {
                    // rowKey = id + time
                    i++;
                    byte[] bytes = r.getRow();

                    int id = Bytes.toInt(bytes);
                    if (id != 1){
                        continue;
                    }

                    long t = Bytes.toLong(bytes, 4);

                    System.err.println(id + ":" + DateUtil.dateToString(new Date(t)));

/*
                    String lat = Bytes.toString(r.getValue(family, Bytes.toBytes("wgs84lat")));
                    String lng = Bytes.toString(r.getValue(family,  Bytes.toBytes("wgs84lng")));
                    System.out.println(lat + ": " + lng);
*/
                    if (i > 20) {

                        break;
                    }
                }
            } finally {
                rs.close();
            }
        }

    }

    @Test
    public void testHBase2() throws Exception {
        org.apache.hadoop.conf.Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "xg153,xg154,xg155");
        config.set("hbase.zookeeper.property.clientPort", "2181");

        long time = System.currentTimeMillis();

        try (Connection connection = ConnectionFactory.createConnection(config)) {
            TableName tableName = TableName.valueOf("obd:vehicle_obd");
            Table table = connection.getTable(tableName);

            byte[] family = Bytes.toBytes("j");
            byte[] start = Bytes.add(Bytes.toBytes(1), Bytes.toBytes(time));

            byte[] mileColumn = Bytes.toBytes("obdMileage");
            byte[] fuelColumn = Bytes.toBytes("obdFuelConsumption");

            Scan scan = new Scan(start);
            scan.addColumn(family, mileColumn);
            scan.addColumn(family, fuelColumn);

            Filter filter = new RowFilter(CompareFilter.CompareOp.LESS, new BinaryComparator(start));
            scan.setFilter(filter);
            scan.setReversed(true);

            ResultScanner rs = table.getScanner(scan);
            try {
                for (Result r : rs) {
                    byte[] bytes = r.getRow();
                    int i = Bytes.toInt(bytes);

                    if (r.containsNonEmptyColumn(family, mileColumn) &&
                            r.containsNonEmptyColumn(family, fuelColumn)) {

                        String mileStr = Bytes.toString(r.getValue(family, mileColumn));
                        String consumStr = Bytes.toString(r.getValue(family, fuelColumn));

                        System.out.println(mileStr + ":" + consumStr);
                        break;
                    }
                }
            } finally {
                rs.close();
            }
        }
    }
}
