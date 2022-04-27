package ebs_lab.Project.data_processing.bolts;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import ebs_lab.Project.model.Publication;

public class SinkBolt extends BaseRichBolt {
	
	private static final long serialVersionUID = 2;
	
	SimpleDateFormat dateFormat;
	
	public SinkBolt(SimpleDateFormat dateFormat) {
		this.dateFormat = dateFormat;
	}

	public void prepare(Map topoConf, TopologyContext context, OutputCollector collector) {
	}

	public void execute(Tuple input) {
		String company = input.getStringByField("company");
		Date date = null;
		try {
			date = this.dateFormat.parse(input.getStringByField("date"));
		} catch (ParseException e) {
			e.printStackTrace();
		}
		double value = input.getDoubleByField("value");
		double drop = input.getDoubleByField("drop");
		double variation = input.getDoubleByField("variation");
		
		Publication publication = new Publication(company, date, value, drop, variation);
		System.out.println(publication);
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}
	
	public void cleanup() {
		System.out.println("Execution finished");
	}
}