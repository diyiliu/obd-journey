package com.tiza;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.tiza.bolt.KafkaTripBolt;
import com.tiza.bolt.ReportTripBolt;
import com.tiza.util.DateUtil;
import com.tiza.util.JacksonUtil;
import org.apache.commons.cli.*;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;

import java.io.InputStream;
import java.util.Date;
import java.util.Properties;

/**
 * Description: Main
 * Author: DIYILIU
 * Update: 2018-07-23 17:06
 */
public class Main {

    public static void main(String[] args) throws Exception {
        System.out.println(JacksonUtil.toJson(args));
        Options options = new Options();
        options.addOption("v", "vehicle", true, "vehicle id");
        options.addOption("d", "day", true, "start time");
        Option subOp = new Option("l", "local", true, "local or cluster[0: cluster,1: local]");
        subOp.setRequired(true);
        options.addOption(subOp);

        CommandLineParser parser = new PosixParser();
        CommandLine cli = parser.parse(options, args);

        // 设置重新消费 时间参数(20180903)
        Date sTime = null;
        if (cli.hasOption("day")) {
            String day = cli.getOptionValue("day");
            sTime = DateUtil.stringToDate(day, "yyyyMMdd");

            if (sTime == null) {
                System.out.println("args error!");
            }
        }
        // 车辆ID
        Long vehicleId = null;
        if (cli.hasOption("vehicle")) {
            vehicleId = Long.parseLong(cli.getOptionValue("vehicle"));
        }

        Properties properties = new Properties();
        try (InputStream in = ClassLoader.getSystemResourceAsStream("obd.properties")) {
            properties.load(in);
            String topic = properties.getProperty("kafka.topic");
            ZkHosts zkHosts = new ZkHosts(properties.getProperty("kafka.zk-host"));

            SpoutConfig spoutConfig = new SpoutConfig(zkHosts, topic, "", "journey");
            spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
            if (sTime != null) {
                spoutConfig.forceFromStart = true;
            }

            TopologyBuilder builder = new TopologyBuilder();
            builder.setSpout("spout", new KafkaSpout(spoutConfig), 1);
            builder.setBolt("bolt1", new KafkaTripBolt(), 1).shuffleGrouping("spout");
            builder.setBolt("bolt2", new ReportTripBolt(), 1).fieldsGrouping("bolt1", new Fields("vehicleId"));

            Config conf = new Config();
            conf.setDebug(false);
            conf.put(backtype.storm.Config.TOPOLOGY_MAX_SPOUT_PENDING, 64);   //限流
            conf.put("zk-quorum", properties.getProperty("hbase.zk-quorum"));
            conf.put("zk-port", properties.getProperty("hbase.zk-port"));
            conf.put("table", properties.getProperty("hbase.table"));
            conf.put("gps-table", properties.getProperty("hbase.gps-table"));
            conf.put("obd-table", properties.getProperty("hbase.obd-table"));
            conf.put("amap-key", properties.getProperty("amap.key"));
            if (sTime != null) {
                conf.put("startTime", sTime.getTime());
            }
            if (vehicleId != null){
                conf.put("vehicleId", vehicleId);
            }

            // 本地模式 + 集群模式
            if (cli.getOptionValue("local").equals("1")) {
                LocalCluster localCluster = new LocalCluster();
                localCluster.submitTopology("obd_journey", conf, builder.createTopology());
            } else {
                StormSubmitter.submitTopology("obd_journey", conf, builder.createTopology());
            }
        }
    }
}
