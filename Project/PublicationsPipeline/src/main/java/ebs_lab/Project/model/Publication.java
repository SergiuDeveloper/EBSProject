package ebs_lab.Project.model;

import java.text.SimpleDateFormat;
import java.util.Date;

public class Publication {
	
	private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-mm-dd");
	
	public Publication(String company, Date date, double value, double drop, double variation) {
		this.company = company;
		this.date = date;
		this.value = value;
		this.drop = drop;
		this.variation = variation;
	}
	
	private String company;
	private Date date;
	private double value;
	private double drop;
	private double variation;
	
	public String getCompany() {
		return this.company;
	}
	
	public Date getDate() {
		return this.date;
	}
	
	public double getValue() {
		return this.value;
	}
	
	public double getDrop() {
		return this.drop;
	}
	
	public double getVariation() {
		return this.variation;
	}
	
	@Override
	public String toString() {
		return String.format("Publication(Company=%s, Date=%s, Value=%f, Drop=%f, Variation=%f)", this.company, DATE_FORMAT.format(this.date), this.value, this.drop, this.variation);
	}
}
