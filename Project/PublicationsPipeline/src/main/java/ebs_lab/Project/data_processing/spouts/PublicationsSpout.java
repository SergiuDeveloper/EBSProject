package ebs_lab.Project.data_processing.spouts;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import ebs_lab.Project.model.Publication;

public class PublicationsSpout extends BaseRichSpout {
	
	private static final long serialVersionUID = 1;
	
	ThreadLocalRandom random;
	SimpleDateFormat dateFormat;
	
	private String[] companies;
	private Date startDate, endDate;
	private double valueMin, valueMax;
	private double dropMin, dropMax;
	private double variationMin, variationMax;
	
	private SpoutOutputCollector collector;
	private String taskIdentifier;
	
	public PublicationsSpout(SimpleDateFormat dateFormat, String[] companies, Date startDate, Date endDate, double valueMin, double valueMax, double dropMin, double dropMax, double variationMin, double variationMax) {
		this.random = ThreadLocalRandom.current();
		this.dateFormat = dateFormat;
		this.companies = companies;
		this.startDate = startDate;
		this.endDate = endDate;
		this.valueMin = valueMin;
		this.valueMax = valueMax;
		this.dropMin = dropMin;
		this.dropMax = dropMax;
		this.variationMin = variationMin;
		this.variationMax = variationMax;
	}

	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		this.taskIdentifier = context.getThisComponentId() + " " + context.getThisTaskId();
	}

	public void nextTuple() {
		Publication publication = this.generatePublication();
		this.collector.emit(new Values(publication.getCompany(), this.dateFormat.format(publication.getDate()), publication.getValue(), publication.getDrop(), publication.getVariation()));
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("company", "date", "value", "drop", "variation"));
	}
	
	private Publication generatePublication() {
		String company = companies[this.random.nextInt(companies.length)];
		Date date = new Date(this.random.nextLong(this.startDate.getTime(), this.endDate.getTime()));
		double value = this.random.nextDouble(this.valueMin, this.valueMax);
		double drop = this.random.nextDouble(this.dropMin, this.dropMax);
		double variation = this.random.nextDouble(this.variationMin, this.variationMax);
		
		return new Publication(company, date, value, drop, variation);
	}
}