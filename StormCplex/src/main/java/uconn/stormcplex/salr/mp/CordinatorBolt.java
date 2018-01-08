package uconn.stormcplex.salr.mp;

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
	public Integer numOfPVGen;
	public Integer numOfHours = 12;

	public Double tap_changer_cost;

	public PVGen[] pvGenArray;
	public Bus[][] busArray;
	public Line[] lineArray;
	public double[][] pbbArray;
	public double[][] transArray;

	public double[][][] se, sf, sp;
	public double[][][] szLp, szLn;
	public Integer[][] sn, sb, sc;
	public double[][][] multB, multLp, slackB, slackLp;
	public double[][] multT, slackT;

	public Integer iteration;
	public int prevIteration;
	public double penalty = 0.1;

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
		declarer.declare(new Fields("id", "numOfBuses", "numOfLines", "numOfScenarios", "numOfPVGen", "multT", "multB",
				"multLp", "slackB", "slackLp", "se", "sf", "sp", "sn", "sb", "sc", "tap_changer_cost", "pvGenArray",
				"busArray", "lineArray", "pbbArray", "transArray", "iteration", "penalty", "sP", "sQ", "sF"));
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
			numOfPVGen = input.getIntegerByField("numOfPVGen");

			tap_changer_cost = (Double) input.getValueByField("tap_changer_cost");

			pvGenArray = (PVGen[]) input.getValueByField("pvGenArray");
			busArray = (Bus[][]) input.getValueByField("busArray");
			lineArray = (Line[]) input.getValueByField("lineArray");
			pbbArray = (double[][]) input.getValueByField("pbbArray");
			transArray = (double[][]) input.getValueByField("transArray");

			iteration = 0;

			multT = new double[numOfScenarios][numOfHours];
			multB = new double[numOfBuses][numOfScenarios][numOfHours];
			multLp = new double[numOfLines][numOfScenarios][numOfHours];

			slackT = new double[numOfScenarios][numOfHours];
			slackB = new double[numOfBuses][numOfScenarios][numOfHours];
			slackLp = new double[numOfLines][numOfScenarios][numOfHours];

			double[][][] sP = new double[numOfLines][numOfScenarios][numOfHours];
			double[][][] sQ = new double[numOfLines][numOfScenarios][numOfHours];
			double[][][] sF = new double[numOfLines][numOfScenarios][numOfHours];

			for (int i = 0; i < numOfLines; i++) {
				for (int j = 0; j < numOfScenarios; j++) {
					for (int k = 0; k < numOfHours; k++) {
						sP[i][j][k] = sQ[i][j][k] = sF[i][j][k] = 0d;
					}
				}
			}

			for (int i = 0; i < numOfBuses; i++) {
				for (int j = 0; j < numOfScenarios; j++) {
					for (int k = 0; k < numOfHours; k++) {
						multB[i][j][k] = 0d;
						slackB[i][j][k] = 0d;
					}
				}
			}

			for (int i = 0; i < numOfLines; i++) {
				for (int j = 0; j < numOfScenarios; j++) {
					for (int k = 0; k < numOfHours; k++) {
						slackLp[i][j][k] = 0d;
						multLp[i][j][k] = 0d;
					}
				}
			}

			for (int i = 0; i < numOfScenarios; i++) {
				for (int j = 0; j < numOfHours; j++) {
					multT[i][j] = 0d;
					slackT[i][j] = 0d;
				}
			}

			se = new double[numOfBuses][numOfScenarios][numOfHours];
			sf = new double[numOfBuses][numOfScenarios][numOfHours];
			sp = new double[numOfBuses][numOfScenarios][numOfHours];
			szLp = new double[numOfLines][numOfScenarios][numOfHours];
			szLn = new double[numOfLines][numOfScenarios][numOfHours];
			sn = new Integer[numOfScenarios][numOfHours];
			sb = new Integer[numOfScenarios][numOfHours];
			sc = new Integer[numOfScenarios][numOfHours];

			for (int i = 0; i < numOfBuses; i++) {
				for (int j = 0; j < numOfScenarios; j++) {
					for (int k = 0; k < numOfHours; k++) {
						se[i][j][k] = 0;
						sf[i][j][k] = 0;
						sp[i][j][k] = 0;
					}
				}
			}

			for (int i = 0; i < numOfScenarios; i++) {
				for (int j = 0; j < numOfHours; j++) {
					sn[i][j] = 0;
					sb[i][j] = 0;
					sc[i][j] = 0;
				}
			}

			penalty = penalty * 1.005;
			if (penalty > 10) {
				penalty = 10;
			}
			if (iteration > 1500 && penalty < 0.01) {
				penalty = 0.01;
			}

			for (int id = 0; id < numOfBuses; id++) {
				collector.emit(new Values(id, numOfBuses, numOfLines, numOfScenarios, numOfPVGen, multT, multB, multLp,
						slackB, slackLp, se, sf, sp, sn, sb, sc, tap_changer_cost, pvGenArray, busArray, lineArray,
						pbbArray, transArray, iteration, penalty, sP, sQ, sF));
				iteration++;
			}
			// collector.emit(new Values(0, numOfBuses, numOfLines, numOfScenarios,
			// numOfPVGen, multT, multB, multLp,
			// slackB, slackLp, se, sf, sp, sn, sb, sc, tap_changer_cost, pvGenArray,
			// busArray, lineArray,
			// pbbArray, transArray, iteration,  , sP, sQ, sF));
			// iteration++;
		} else

		{
			double q = (double) input.getValueByField("q");
			double[][][] sP = (double[][][]) input.getValueByField("sP");
			double[][][] sQ = (double[][][]) input.getValueByField("sQ");
			double[][][] sF = (double[][][]) input.getValueByField("sF");
			int lastIteration = input.getIntegerByField("lastIteration");
			int id = (int) input.getValueByField("id");
			double[][][] E = (double[][][]) input.getValueByField("eVal");
			double[][][] F = (double[][][]) input.getValueByField("fVal");

			penalty = penalty * 1.005;
			if (penalty > 10) {
				penalty = 10;
			}
			if (lastIteration > 1500 && penalty < 0.01) {
				penalty = 0.01;
			}

			for (int k = 0; k < numOfHours; k++) {
				for (int j = 0; j < numOfScenarios; j++) {
					se[id][j][k] = E[id][j][k];
					sf[id][j][k] = F[id][j][k];
				}
			}

			sp = (double[][][]) input.getValueByField("pVal");

			szLp = (double[][][]) input.getValueByField("szLp");
			szLn = (double[][][]) input.getValueByField("szLn");
			sn = (Integer[][]) input.getValueByField("sn");
			sb = (Integer[][]) input.getValueByField("sb");
			sc = (Integer[][]) input.getValueByField("sc");

			for (int k = 0; k < numOfHours; k++) {
				for (int j = 0; j < numOfScenarios; j++) {
					for (int i = 0; i < numOfBuses; i++) {
						slackB[i][j][k] = 0;
					}
				}
			}

			for (int i = 0; i < numOfBuses; i++) {
				for (int j = 0; j < numOfScenarios; j++) {
					for (int k = 0; k < numOfHours; k++) {
						slackB[i][j][k] += sp[i][j][k];
					}
				}
			}
			// System.out.println("slackbsp"+Arrays.deepToString(slackB));
			for (int i = 0; i < numOfBuses; i++) {
				for (int j = 0; j < numOfScenarios; j++) {
					for (int k = 0; k < numOfHours; k++) {
						slackB[i][j][k] -= busArray[i][k].load;
					}
				}
			}
			// System.out.println("slackbload"+Arrays.deepToString(slackB));
			for (int i = 0; i < numOfPVGen; i++) {
				for (int j = 0; j < numOfScenarios; j++) {
					for (int k = 0; k < numOfHours; k++) {
						slackB[pvGenArray[i].busNum][j][k] += pvGenArray[i].pvGen[j];
					}
				}
			}
			// System.out.println("slackbpvgen"+Arrays.deepToString(slackB));
			for (int i = 0; i < numOfLines; i++) {
				for (int j = 0; j < numOfScenarios; j++) {
					for (int k = 0; k < numOfHours; k++) {
						int srcNodeIndex = lineArray[i].i;
						int destNodeIndex = lineArray[i].j;
						slackB[srcNodeIndex][j][k] -= sP[i][j][k];
						slackB[destNodeIndex][j][k] += sP[i][j][k];
						slackLp[i][j][k] = sF[i][j][k] - 2.25d + szLp[i][j][k];
					}
				}
			}
			// System.out.println("slackbpexpr"+Arrays.deepToString(slackB));
			for (int j = 0; j < numOfScenarios; j++) {
				for (int k = 0; k < numOfHours; k++) {
					slackT[j][k] = se[0][j][k] * pvGenArray[0].pvVol[j] * (a0 + sn[j][k] * delta_a)
							- pvGenArray[0].pvGen[j]
							+ ss * (a0 + sn[j][k] * delta_a) * (a0 + sn[j][k] * delta_a) * pvGenArray[0].pvGen[j];

				}
				// System.out.println("se: "+se[id][j][k]);
			}

			norm = 0;
			for (int k = 0; k < numOfHours; k++) {
				for (int j = 0; j < numOfScenarios; j++) {
					for (int i = 0; i < numOfBuses; i++) {
						norm += slackB[i][j][k] * slackB[i][j][k];
					}
				}
			}
			// System.out.println("norm0: " + norm);
			for (int k = 0; k < numOfHours; k++) {
				for (int j = 0; j < numOfScenarios; j++) {
					for (int i = 0; i < numOfLines; i++) {
						norm += slackLp[i][j][k] * slackLp[i][j][k];
					}
				}
			}
			// System.out.println("norm1: " + norm);
			for (int k = 0; k < numOfHours; k++) {
				for (int j = 0; j < numOfScenarios; j++) {
					norm += slackT[j][k] * slackT[j][k];
				}
			}

			// System.out.println("norm2: " + norm);
			// System.out.println("slackB: " + Arrays.deepToString(slackB));
			// System.out.println("slackLp: " + Arrays.deepToString(slackLp));
			// System.out.println("slackT: " + Arrays.deepToString(slackT));

			step = 0.9995 * oldstep * Math.sqrt(oldnorm / norm);

			for (int k = 0; k < numOfHours; k++) {
				for (int j = 0; j < numOfScenarios; j++) {
					for (int i = 0; i < numOfBuses; i++) {
						multB[i][j][k] = multB[i][j][k] + step * slackB[i][j][k];
					}
					for (int i = 0; i < numOfLines; i++) {
						multLp[i][j][k] = multLp[i][j][k] + step * slackLp[i][j][k];
					}
					multT[j][k] = multT[j][k] + step * slackT[j][k];
				}
			}

			oldnorm = norm;
			oldstep = step;

			long temp_time = System.currentTimeMillis() - current_time;
			System.out.println("lastIteration = " + lastIteration + " id = " + id + " q = " + q + " time = " + temp_time
					+ " norm = " + norm + " step = " + step + " penalty = " + penalty);
			// + " multB = " + Arrays.deepToString(multB));
			// collector.emit(new Values(id, numOfBuses, numOfLines, numOfScenarios,
			// numOfPVGen, multT, multB, multLp,
			// slackB, slackLp, se, sf, sp, sn, sb, sc, tap_changer_cost, pvGenArray,
			// busArray, lineMap, pbbArray,
			// transArray, iteration, penalty, pExprMap, qExprMap, fExprMap));
			// System.out.println("sP: " + Arrays.deepToString(sP));
			id = (id + 1) % 3;
			collector.emit(new Values(id, numOfBuses, numOfLines, numOfScenarios, numOfPVGen, multT, multB, multLp,
					slackB, slackLp, se, sf, sp, sn, sb, sc, tap_changer_cost, pvGenArray, busArray, lineArray,
					pbbArray, transArray, iteration, penalty, sP, sQ, sF));
			iteration++;
		}
	}

	@Override
	public void prepare(Map arg0, TopologyContext arg1) {
		// TODO Auto-generated method stub
		prevIteration = -1;

	}

}
