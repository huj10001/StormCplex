package uconn.stormcplex.salr;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

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

public class CordinatorBolt implements IBasicBolt {
	public Integer numOfBuses;
	public Integer numOfLines;
	public Integer numOfScenarios;
	public double[][] se, sf, sp, szLp, szLn;
	public Integer[] sn, sb, sc;
	public double[][] multB, multLp, multLn;
	public double[][] slackB, slackLp, slackLn;
	public double[] multT, slackT, PVgen;
	public Map<Line, Double> lineMap;
	public double[] busMult, busP;

	public Integer iteration;
	double penalty = 0.001;
	public int prevIteartion;

	double Lagrangian = -1;

	double step = 2 * 3.5 / 100d;
	double oldstep = 2 * 3.5 / 100d;

	double norm = 100d;
	double oldnorm = 100d;

	long current_time;

	double a0 = 1d;
	double delta_a = 0.1;
	double ss = 0.75;

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("id", "numOfBuses", "numOfLines", "numOfScenarios", "multT", "multB", "multLn",
				"multLp", "se", "sf", "sp", "sn", "sb", "sc", "PVgen", "busMult", "busP", "lineMap", "iteration",
				"penalty", "pExprMap", "qExprMap", "fExprMap"));
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
		if (input.getSourceComponent().equals(MyConfig.DATA_COLLECTOR_SPOUT_NAME)) {
			current_time = System.currentTimeMillis();
			numOfBuses = input.getIntegerByField("numOfBuses");
			numOfLines = input.getIntegerByField("numOfLines");
			numOfScenarios = input.getIntegerByField("numOfScenarios");
			multT = (double[]) input.getValueByField("multT");
			multB = (double[][]) input.getValueByField("multB");
			multLn = (double[][]) input.getValueByField("multLn");
			multLp = (double[][]) input.getValueByField("multLp");
			slackT = (double[]) input.getValueByField("slackT");
			slackB = (double[][]) input.getValueByField("slackB");
			slackLn = (double[][]) input.getValueByField("slackLn");
			slackLp = (double[][]) input.getValueByField("slackLp");

			se = (double[][]) input.getValueByField("se");
			sf = (double[][]) input.getValueByField("sf");
			sp = (double[][]) input.getValueByField("sp");
			szLp = (double[][]) input.getValueByField("szLp");
			szLn = (double[][]) input.getValueByField("szLn");
			sn = (Integer[]) input.getValueByField("sn");
			sb = (Integer[]) input.getValueByField("sb");
			sc = (Integer[]) input.getValueByField("sc");

			PVgen = (double[]) input.getValueByField("PVgen");
			busMult = (double[]) input.getValueByField("busMult");
			busP = (double[]) input.getValueByField("busP");

			lineMap = (Map<Line, Double>) input.getValueByField("lineMap");

			iteration = 0;

			penalty = penalty * 1.005;
			if (penalty > 0.5) {
				penalty = 0.5;
			}

			Map<Line, double[]> pExprMap = new HashMap<Line, double[]>();
			Map<Line, double[]> qExprMap = new HashMap<Line, double[]>();
			Map<Line, double[]> fExprMap = new HashMap<Line, double[]>();
			// Line[] lineList = new Line[numOfLines];
			// int k = 0;
			for (Entry<Line, Double> entry : lineMap.entrySet()) {
				Line newline = entry.getKey();
				pExprMap.put(newline, new double[numOfScenarios]);
				qExprMap.put(newline, new double[numOfScenarios]);
				fExprMap.put(newline, new double[numOfScenarios]);
			}

			for (int id = 0; id < numOfBuses; id++) {
				collector.emit(new Values(id, numOfBuses, numOfLines, numOfScenarios, multT, multB, multLn, multLp, se,
						sf, sp, sn, sb, sc, PVgen, busMult, busP, lineMap, iteration, penalty, pExprMap, qExprMap,
						fExprMap));
				iteration++;
			}
		} else {
			penalty = penalty * 1.005;
			if (penalty > 0.5) {
				penalty = 0.5;
			}
			double q = (double) input.getValueByField("q");
			Map<Line, double[]> pExprMap = (Map<Line, double[]>) input.getValueByField("pExprMap");
			Map<Line, double[]> qExprMap = (Map<Line, double[]>) input.getValueByField("qExprMap");
			Map<Line, double[]> fExprMap = (Map<Line, double[]>) input.getValueByField("fExprMap");
			int lastIteartion = input.getIntegerByField("lastIteartion");
			int id = (int) input.getValueByField("id");
			////////////////////////////////////////////////
			// if (lastIteartion > prevIteartion) {
			// prevIteartion = lastIteartion;
			///////////////////////////////////////////////////////////////////////////////
			se[id] = (double[]) input.getValueByField("eVal");
			sf[id] = (double[]) input.getValueByField("fVal");

			double[][] pVal = (double[][]) input.getValueByField("pVal");
			szLp = (double[][]) input.getValueByField("szLp");
			szLn = (double[][]) input.getValueByField("szLn");
			sn = (Integer[]) input.getValueByField("sn");
			sb = (Integer[]) input.getValueByField("sb");
			sc = (Integer[]) input.getValueByField("sc");

//			if (lastIteartion > 10) {
//				sp[id] = pVal[id];
//			} else {
				for (int i = 0; i < numOfBuses; i++) {
					sp[i] = pVal[i];
				}
//			}

			Line[] lineList = new Line[numOfLines];
			double[][] doubleListp = new double[numOfLines][numOfScenarios];
			double[][] doubleListq = new double[numOfLines][numOfScenarios];
			double[][] doubleListf = new double[numOfLines][numOfScenarios];

			int k = 0;
			for (Entry<Line, double[]> entry : pExprMap.entrySet()) {
				lineList[k] = entry.getKey();
				doubleListp[k] = entry.getValue();
				doubleListq[k] = qExprMap.get(lineList[k]);
				doubleListf[k] = fExprMap.get(lineList[k]);
				k++;
			}

			for (int j = 0; j < numOfScenarios; j++) {
				for (int i = 0; i < numOfBuses; i++) {
					slackB[i][j] = 0;
				}
			}
			
			double[] pExpr = new double[numOfBuses];
			double[] qExpr = new double[numOfBuses];
			double[] fExpr = new double[numOfBuses];
			for (int j = 0; j < numOfScenarios; j++) {
				for (int i = 0; i < numOfBuses; i++) {
					slackB[i][j] += (sp[i][j] - busP[i]);
				}

				slackB[0][j] += PVgen[j];

				for (int i = 0; i < numOfLines; i++) {
					int srcNodeIndex = lineList[i].i;
					int destNodeIndex = lineList[i].j;
					pExpr = doubleListp[i];
					qExpr = doubleListq[i];
					fExpr = doubleListf[i];
					slackB[srcNodeIndex][j] += pExpr[j];
					slackB[destNodeIndex][j] -= pExpr[j];
					slackLp[i][j] = pExpr[j] * pExpr[j] + qExpr[j] * qExpr[j] - 2.25d + szLp[i][j];
					// slackLn[i][j] = -pExpr[j] - 2.25d + szLn[i][j];

					System.out.println("pexpr: ["+srcNodeIndex+","+destNodeIndex+"]"+Arrays.toString(pExpr));
				}
				slackT[j] = se[0][j] * Math.sqrt(PVgen[j]) * (a0 + sn[j] * delta_a) - PVgen[j]
						+ ss * (a0 + sn[j] * delta_a) * (a0 + sn[j] * delta_a) * PVgen[j];
			}
			
			System.out.println("pexpr: "+Arrays.toString(pExpr));
			System.out.println("qexpr: "+Arrays.toString(qExpr));
			System.out.println("fexpr: "+Arrays.toString(fExpr));
			
			norm = 0;
			for (int j = 0; j < numOfScenarios; j++) {
				for (int i = 0; i < numOfBuses; i++) {
					norm += slackB[i][j] * slackB[i][j];
				}
				for (int i = 0; i < numOfLines; i++) {
					norm += slackLp[i][j] * slackLp[i][j];
					// norm += slackLn[i][j] * slackLn[i][j];
				}
				norm += slackT[j] * slackT[j];
				norm += 0.00000001;
			}
			System.out.println("slackB: "+Arrays.deepToString(slackB));
			System.out.println("slackLp: "+Arrays.deepToString(slackLp));
			System.out.println("slackT: "+Arrays.toString(slackT));
			// norm += 0.00000001;

			// double rat = oldstep / step * Math.sqrt(oldnorm / norm);

			step = 0.9995 * oldstep * Math.sqrt(oldnorm / norm);

			for (int j = 0; j < numOfScenarios; j++) {
				for (int i = 0; i < numOfBuses; i++) {
					multB[i][j] = multB[i][j] + step * slackB[i][j];
				}
				for (int i = 0; i < numOfLines; i++) {
					multLp[i][j] = multLp[i][j] + step * slackLp[i][j];
					multLn[i][j] = multLn[i][j] + step * slackLn[i][j];
				}
				multT[j] = multT[j] + step * slackT[j];
			}

			oldnorm = norm;
			oldstep = step;

			long temp_time = System.currentTimeMillis() - current_time;
			System.out.println("lastIteartion = " + lastIteartion + " id = " + id + " q = " + q + " time = " + temp_time
					+ " norm = " + norm + " step = " + step + " multB = " + multB[0][0] + " " + multB[0][1] + " "
					+ multB[0][2] + " " + multB[0][3] + " " + multB[0][4] + " " + multB[1][0] + " " + multB[1][1] + " "
					+ multB[1][2] + " " + multB[1][3] + " " + multB[1][4] + " " + multB[2][0] + " " + multB[2][1] + " "
					+ multB[2][2] + " " + multB[2][3] + " " + multB[2][4] + " ");
//			if (lastIteartion == 1) {
//				System.exit(0);
//			}
			//////////////////////////////////////////////////////
			// }
			///////////////////////////////////////////////////////
			collector.emit(new Values(id, numOfBuses, numOfLines, numOfScenarios, multT, multB, multLn, multLp, se, sf,
					sp, sn, sb, sc, PVgen, busMult, busP, lineMap, iteration, penalty, pExprMap, qExprMap, fExprMap));
			
			iteration++;
		}
	}

	@Override
	public void prepare(Map arg0, TopologyContext arg1) {
		// TODO Auto-generated method stub
		prevIteartion = -1;

	}

}
