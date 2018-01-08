package uconn.stormcplex.salr.mp;

import java.io.Serializable;

public class PVGen implements Serializable {

	public String busName;
	public Integer busNum;

	public double[] pvGen;
	public double[] pvVol;

	public PVGen(String busName, Integer busNum, double[] pvGen, double[] pvVol) {

		this.busName = busName;
		this.busNum = busNum;
		this.pvGen = pvGen;
		this.pvVol = pvVol;
	}

}