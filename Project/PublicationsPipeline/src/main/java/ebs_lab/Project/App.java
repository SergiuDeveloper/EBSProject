package ebs_lab.Project;

import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;

import ebs_lab.Project.data_processing.bolts.SinkBolt;
import ebs_lab.Project.data_processing.spouts.PublicationsSpout;

public class App 
{
	
	private static final String TOPOLOGY_ID = "main_topology";
	private static final String PUBLICATIONS_SPOUT_ID = "publications_spout";
	private static final String SINK_BOLT_ID = "sink_bolt";
	
	private static final Map<String, Integer> WORKERS_MAP = new HashMap<String, Integer>() {{
		put(PUBLICATIONS_SPOUT_ID, 2);
		put(SINK_BOLT_ID, 3);
	}};
	
	private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-mm-dd");
	private static final String[] COMPANIES = new String[] {"Amazon", "Google", "Microsoft", "Apple", "Tesla", "SpaceX", "Twitter", "Bolt"};
	private static final Date START_DATE = new Date(Instant.now().minus(90, ChronoUnit.DAYS).toEpochMilli());
	private static final Date END_DATE = new Date();
	private static final double VALUE_MIN = 0;
	private static final double VALUE_MAX = 100;
	private static final double DROP_MIN = 5;
	private static final double DROP_MAX = 500;
	private static final double VARIATION_MIN = 0;
	private static final double VARIATION_MAX = 100;
	
    public static void main(String[] args) throws Exception
    {
    	TopologyBuilder builder = new TopologyBuilder();
    	PublicationsSpout publicationsSpout = new PublicationsSpout(DATE_FORMAT, COMPANIES, START_DATE, END_DATE, VALUE_MIN, VALUE_MAX, DROP_MIN, DROP_MAX, VARIATION_MIN, VARIATION_MAX);
    	SinkBolt sinkBolt = new SinkBolt(DATE_FORMAT);
    	  	
    	builder.setSpout(PUBLICATIONS_SPOUT_ID, publicationsSpout, WORKERS_MAP.get(PUBLICATIONS_SPOUT_ID));
    	builder.setBolt(SINK_BOLT_ID, sinkBolt, WORKERS_MAP.get(SINK_BOLT_ID)).shuffleGrouping(PUBLICATIONS_SPOUT_ID);
    	
    	Config config = new Config();
    	
    	LocalCluster cluster = new LocalCluster();
    	StormTopology topology = builder.createTopology();
    	
    	config.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, 1024);
    	config.put(Config.TOPOLOGY_DISRUPTOR_BATCH_SIZE, 1);
    	
    	cluster.submitTopology(TOPOLOGY_ID, config, topology);
    	
    	try {
			Thread.sleep(20000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

    	cluster.killTopology(TOPOLOGY_ID);
    	cluster.shutdown();
    }
}