package uconn.stormcplex.update;

import java.util.Map;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import ilog.concert.IloException;
import ilog.concert.IloIntExpr;
import ilog.concert.IloIntVar;
import ilog.concert.IloNumExpr;
import ilog.concert.IloNumVar;
import ilog.cplex.IloCplex;

public class CordinatorBolt_update implements IBasicBolt {
	public Integer numOfBuses;
	public Integer numOfLines;
	public Integer numOfScenarios;
	public double[][] se, sf, sp, szLp, szLn;
	public double[][] multB, multLp, multLn;
	public double[][] slackB, slackLp, slackLn;
	public double[] PVgen;

	public Integer iteration;

	double Lagrangian = -1;

	double step = 3.5 / 10;
	double oldstep = 3.5 / 10;

	double norm = 100.0;
	double oldnorm = 100.0;

	double penalty = 1 / 100000;

	long current_time;

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("id", "numOfBuses", "numOfLines", "numOfScenarios", "multB", "multLn", "multLp",
				"se", "sf", "sp", "szLp", "szLn", "PVgen", "iteration"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		// TODO Auto-generated method stub
		if (input.getSourceComponent().equals("DataCollectorSpout_update")) {
			current_time = System.currentTimeMillis();
			numOfBuses = input.getIntegerByField("numOfBuses");
			numOfLines = input.getIntegerByField("numOfLines");
			numOfScenarios = input.getIntegerByField("numOfScenarios");
			multB = (double[][]) input.getValueByField("multB");
			multLn = (double[][]) input.getValueByField("multLn");
			multLp = (double[][]) input.getValueByField("multLp");
			slackB = (double[][]) input.getValueByField("slackB");
			slackLn = (double[][]) input.getValueByField("slackLn");
			slackLp = (double[][]) input.getValueByField("slackLp");

			se = (double[][]) input.getValueByField("se");
			sf = (double[][]) input.getValueByField("sf");
			sp = (double[][]) input.getValueByField("sp");
			szLp = (double[][]) input.getValueByField("szLp");
			szLn = (double[][]) input.getValueByField("szLn");

			PVgen = (double[]) input.getValueByField("PVgen");
			iteration = 0;

			for (int id = 0; id < numOfBuses; id++) {
				collector.emit(new Values(id, numOfBuses, numOfLines, numOfScenarios, multB, multLn, multLp, se, sf, sp,
						szLp, szLn, PVgen, iteration));
				iteration++;
			}
		} else {
			double q = (double) input.getValueByField("q");
			double[] p12 = (double[]) input.getValueByField("p12");
			double[] p13 = (double[]) input.getValueByField("p13");
			double[] p32 = (double[]) input.getValueByField("p32");
			double[][] eVal = (double[][]) input.getValueByField("eVal");
			double[][] fVal = (double[][]) input.getValueByField("fVal");
			double[][] pVal = (double[][]) input.getValueByField("pVal");
			double[][] szLp = (double[][]) input.getValueByField("szLp");
			double[][] szLn = (double[][]) input.getValueByField("szLn");
			int id = (int) input.getValueByField("id");

			se[id] = eVal[id];
			sf[id] = fVal[id];
			sp[id] = pVal[id];

			for (int i = 0; i < numOfScenarios; i++) {
				slackB[0][i] = p12[i] + sp[0][i] - p13[i] + PVgen[i] - 10;
				slackB[1][i] = p32[i] + sp[1][i] - p12[i] - 5;
				slackB[2][i] = p13[i] + sp[2][i] - p32[i] - 5;

				slackLp[0][i] = p12[i] - 1.5 + szLp[0][i];
				slackLp[1][i] = p13[i] - 1.5 + szLp[1][i];
				slackLp[2][i] = p32[i] - 1.5 + szLp[2][i];
				slackLn[0][i] = -p12[i] - 1.5 + szLn[0][i];
				slackLn[1][i] = -p13[i] - 1.5 + szLn[1][i];
				slackLn[2][i] = -p32[i] - 1.5 + szLn[2][i];
			}

			norm = 0;
			for (int i = 0; i < numOfScenarios; i++) {
				norm += slackB[0][i] * slackB[0][i] + slackB[1][i] * slackB[1][i] + slackB[2][i] * slackB[2][i];
				norm += slackLp[0][i] * slackLp[0][i] + slackLp[1][i] * slackLp[1][i] + slackLp[2][i] * slackLp[2][i];
				norm += slackLn[0][i] * slackLn[0][i] + slackLn[1][i] * slackLn[1][i] + slackLn[2][i] * slackLn[2][i];// +
			}

			double rat = oldstep / step * Math.sqrt(oldnorm / norm);

			step = 0.9975 * oldstep * Math.sqrt(oldnorm / norm);

			for (int i = 0; i < numOfScenarios; i++) {
				multB[0][i] = multB[0][i] + step * slackB[0][i];
				multB[1][i] = multB[1][i] + step * slackB[1][i];
				multB[2][i] = multB[2][i] + step * slackB[2][i];

				multLp[0][i] = multLp[0][i] + step * slackLp[0][i];
				multLp[1][i] = multLp[1][i] + step * slackLp[1][i];
				multLp[2][i] = multLp[2][i] + step * slackLp[2][i];

				multLn[0][i] = multLn[0][i] + step * slackLn[0][i];
				multLn[1][i] = multLn[1][i] + step * slackLn[1][i];
				multLn[2][i] = multLn[2][i] + step * slackLn[2][i];
			}

			oldnorm = norm;
			oldstep = step;
			/*
			 * System.out.println(id + "	" + penalty + "	" + q + "	" +
			 * Lagrangian + "	" + step + "	" + norm + "	" + sp[0] +
			 * "	" + sp[1] + "	" + sp[2] + "	" + p12 + "	" + p13 + "	" +
			 * p32 + "	" + multB + "	" + multLp + "	" + multLn + "	" + se +
			 * "	" + sf);
			 */
			long temp_time = System.currentTimeMillis() - current_time;
			System.out.println("id = " + id + " " + "q = " + q + " " + " " + "time = " + temp_time + " norm = " + norm);

			collector.emit(new Values(id, numOfBuses, numOfLines, numOfScenarios, multB, multLn, multLp, se, sf, sp,
					szLp, szLn, PVgen, iteration));
			iteration++;
		}
	}

	@Override
	public void prepare(Map arg0, TopologyContext arg1) {
		// TODO Auto-generated method stub

	}

}
