package uconn.stormcplex.salr;

import java.io.Serializable;

public class Bus implements Serializable {
	public String busName;
	public Integer busNum;
	public double load;
	public double cost;

	public Bus(String busName, Integer busNum, double load, double cost) {
		this.busName = busName;
		this.busNum = busNum;
		this.load = load;
		this.cost = cost;
	}
}