package storm.starter;

import java.util.Map;
import org.json.simple.JSONValue;
import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.Nimbus.Client;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;

public class RunningClusterTopology {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        Config conf = new Config();
        conf.put(Config.NIMBUS_HOST, "155.69.149.213");
        conf.setDebug(true);
        Map storm_conf = Utils.readStormConfig();
        storm_conf.put("nimbus.host", "155.69.149.213");
        Client client = NimbusClient.getConfiguredClient(storm_conf)
                .getClient();
        String inputJar = "C:\\workspace\\TestStormRunner\\target\\TestStormRunner-0.0.1-SNAPSHOT-jar-with-dependencies.jar";
        NimbusClient nimbus = new NimbusClient(storm_conf, "155.69.149.213",
                6627);
        // upload topology jar to Cluster using StormSubmitter
        String uploadedJarLocation = StormSubmitter.submitJar(storm_conf,
                inputJar);
        try {
            String jsonConf = JSONValue.toJSONString(storm_conf);
            nimbus.getClient().submitTopology("testtopology",
                    uploadedJarLocation, jsonConf, builder.createTopology());
        } catch (AlreadyAliveException ae) {
            ae.printStackTrace();
        }
        Thread.sleep(60000);
    }
}