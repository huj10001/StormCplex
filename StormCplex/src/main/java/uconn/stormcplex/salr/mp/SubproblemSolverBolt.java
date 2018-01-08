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
import ilog.concert.IloIntVar;
import ilog.concert.IloNumExpr;
import ilog.concert.IloNumVar;
import ilog.cplex.IloCplex;

public class SubproblemSolverBolt implements IBasicBolt {
	IloCplex cplex;
	public Integer numOfBuses;
	public Integer numOfLines;
	public Integer numOfScenarios;
	public Integer numOfPVGen;
	public Integer numOfHours = 12;
	public Double tap_changer_cost;
	public PVGen[] pvGenArray;
	public Bus[][] busArray;
	public Line[] lineArray;
	public double[][][] sP;
	public double[][][] sQ;
	public double[][][] sF;
	public double[][] pbbArray;
	public double[][] transArray;
	public double[][][] se, sf, sp;
	public Integer[][] sn, sb, sc;
	public double[][][] multB, multLp;
	public double[][] multT;
	public double[][][] slackB, slackLp;
	public Integer iteration;
	public double penalty;
	double a0 = 1d;
	double delta_a = 0.1;
	double ss = 0.75;
	int initiallevel = 0;

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("q", "sP", "sQ", "sF", "eVal", "fVal", "pVal", "szLp", "szLn", "sn", "sb", "sc",
				"id", "lastIteration"));
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
		int id;
		id = input.getIntegerByField("id");
		numOfBuses = input.getIntegerByField("numOfBuses");
		numOfLines = input.getIntegerByField("numOfLines");
		numOfScenarios = input.getIntegerByField("numOfScenarios");
		numOfPVGen = input.getIntegerByField("numOfPVGen");

		multT = (double[][]) input.getValueByField("multT");
		multB = (double[][][]) input.getValueByField("multB");
		multLp = (double[][][]) input.getValueByField("multLp");
		slackB = (double[][][]) input.getValueByField("slackB");
		slackLp = (double[][][]) input.getValueByField("slackLp");
		se = (double[][][]) input.getValueByField("se");
		sf = (double[][][]) input.getValueByField("sf");
		sp = (double[][][]) input.getValueByField("sp");
		sn = (Integer[][]) input.getValueByField("sn");
		sb = (Integer[][]) input.getValueByField("sb");
		sc = (Integer[][]) input.getValueByField("sc");

		tap_changer_cost = (Double) input.getValueByField("tap_changer_cost");

		pvGenArray = (PVGen[]) input.getValueByField("pvGenArray");
		busArray = (Bus[][]) input.getValueByField("busArray");

		pbbArray = (double[][]) input.getValueByField("pbbArray");
		transArray = (double[][]) input.getValueByField("transArray");
		lineArray = (Line[]) input.getValueByField("lineArray");
		penalty = (double) input.getValueByField("penalty");
		iteration = input.getIntegerByField("iteration");

		sP = (double[][][]) input.getValueByField("sP");
		sQ = (double[][][]) input.getValueByField("sQ");
		sF = (double[][][]) input.getValueByField("sF");

		try {
			cplex = new IloCplex();
			cplex.setOut(null);

			IloNumVar[][][] eVar = new IloNumVar[numOfBuses][numOfScenarios][numOfHours];
			IloNumVar[][][] fVar = new IloNumVar[numOfBuses][numOfScenarios][numOfHours];
			IloNumVar[][][] pVar = new IloNumVar[numOfBuses][numOfScenarios][numOfHours];
			IloNumVar[][][] zLpVar = new IloNumVar[numOfLines][numOfScenarios][numOfHours];

			for (int i = 0; i < numOfBuses; i++) {
				for (int j = 0; j < numOfScenarios; j++) {
					for (int k = 0; k < numOfHours; k++) {
						pVar[i][j][k] = cplex.numVar(2, 50, "p" + i + j + k);
						cplex.add(pVar[i][j][k]);
					}
				}
			}

			for (int i = 0; i < numOfLines; i++) {
				for (int j = 0; j < numOfScenarios; j++) {
					for (int k = 0; k < numOfHours; k++) {
						zLpVar[i][j][k] = cplex.numVar(0, 10, "zlp" + i + j + k);
						cplex.add(zLpVar[i][j][k]);
					}
				}
			}

			IloIntVar[][] nVar = new IloIntVar[numOfScenarios][numOfHours];
			IloIntVar[][] bVar = new IloIntVar[numOfScenarios][numOfHours];
			IloIntVar[][] cVar = new IloIntVar[numOfScenarios][numOfHours];
			IloIntVar[][] z1Var = new IloIntVar[numOfScenarios][numOfHours];

			for (int i = 0; i < numOfScenarios; i++) {
				for (int j = 0; j < numOfHours; j++) {
					for (int k = 0; k < numOfBuses; k++) {
						eVar[k][i][j] = cplex.numVar(-2.5, 2.5, "e" + k + i + j);
						cplex.add(eVar[k][i][j]);
						fVar[k][i][j] = cplex.numVar(-2.5, 2.5, "f" + k + i + j);
						cplex.add(fVar[k][i][j]);
					}

					nVar[i][j] = cplex.intVar(0, 5, "n" + i + j);
					cplex.add(nVar[i][j]);
					bVar[i][j] = cplex.intVar(0, 10, "b" + i + j);
					cplex.add(bVar[i][j]);
					cVar[i][j] = cplex.intVar(0, 10, "c" + i + j);
					cplex.add(cVar[i][j]);
					z1Var[i][j] = cplex.boolVar("z1" + i + j);
					cplex.add(z1Var[i][j]);
				}
			}

			IloNumExpr[][][] busPExpr = new IloNumExpr[numOfBuses][numOfScenarios][numOfHours];
			for (int i = 0; i < numOfBuses; i++) {
				for (int j = 0; j < numOfScenarios; j++) {
					for (int k = 0; k < numOfHours; k++) {
						busPExpr[i][j][k] = cplex.numExpr();
						busPExpr[i][j][k] = cplex.diff(busPExpr[i][j][k], pVar[i][j][k]);
						busPExpr[i][j][k] = cplex.diff(busPExpr[i][j][k], -busArray[i][k].load);
					}
				}
			}

			for (int k = 0; k < numOfHours; k++) {
				for (int j = 0; j < numOfScenarios; j++) {
					for (int i = 0; i < numOfPVGen; i++) {
						busPExpr[pvGenArray[i].busNum][j][k] = cplex.diff(busPExpr[pvGenArray[i].busNum][j][k],
								pvGenArray[i].pvGen[j]);
					}
				}
			}

			IloNumExpr[][][] PExpr = new IloNumExpr[numOfLines][numOfScenarios][numOfHours];
			IloNumExpr[][][] QExpr = new IloNumExpr[numOfLines][numOfScenarios][numOfHours];
			IloNumExpr[][][] lineExprp = new IloNumExpr[numOfLines][numOfScenarios][numOfHours];

			for (int i = 0; i < numOfScenarios; i++) {
				for (int j = 0; j < numOfHours; j++) {
					if (iteration >= 10) {
						if (id != 0) {
							cplex.addEq(nVar[i][j], sn[i][j]);
							cplex.addEq(bVar[i][j], sb[i][j]);
							cplex.addEq(cVar[i][j], sc[i][j]);
						}
					}
				}
			}

			IloNumVar[][][] pexprvar = new IloNumVar[numOfLines][numOfScenarios][numOfHours];
			IloNumVar[][][] qexprvar = new IloNumVar[numOfLines][numOfScenarios][numOfHours];
			IloNumVar[][][] fexprvar = new IloNumVar[numOfLines][numOfScenarios][numOfHours];
			for (int m = 0; m < numOfLines; m++) {
				for (int n = 0; n < numOfScenarios; n++) {
					for (int k = 0; k < numOfHours; k++) {
						pexprvar[m][n][k] = cplex.numVar(-20, 20, "P" + m + n + k);
						cplex.add(pexprvar[m][n][k]);
						qexprvar[m][n][k] = cplex.numVar(-20, 20, "Q" + m + n + k);
						cplex.add(qexprvar[m][n][k]);
						fexprvar[m][n][k] = cplex.numVar(-20000, 20000, "F" + m + n + k);
						cplex.add(fexprvar[m][n][k]);
					}
				}
			}

			for (int i = 0; i < numOfLines; i++) {
				for (int j = 0; j < numOfScenarios; j++) {
					for (int k = 0; k < numOfHours; k++) {
						int srcNodeIndex = lineArray[i].i;
						int destNodeIndex = lineArray[i].j;
						PExpr[i][j][k] = cplex.numExpr();
						QExpr[i][j][k] = cplex.numExpr();
						lineExprp[i][j][k] = cplex.numExpr();
						if (iteration >= 9) {
							if (srcNodeIndex == id) {
								PExpr[i][j][k] = cplex.sum(PExpr[i][j][k],
										cplex.prod(eVar[srcNodeIndex][j][k], lineArray[i].b * se[destNodeIndex][j][k]));
								PExpr[i][j][k] = cplex.sum(PExpr[i][j][k], cplex.prod(eVar[srcNodeIndex][j][k],
										lineArray[i].g * -sf[destNodeIndex][j][k]));
								PExpr[i][j][k] = cplex.sum(PExpr[i][j][k],
										cplex.prod(fVar[srcNodeIndex][j][k], lineArray[i].g * se[destNodeIndex][j][k]));
								PExpr[i][j][k] = cplex.sum(PExpr[i][j][k],
										cplex.prod(fVar[srcNodeIndex][j][k], lineArray[i].b * sf[destNodeIndex][j][k]));

								QExpr[i][j][k] = cplex.sum(QExpr[i][j][k],
										cplex.prod(fVar[srcNodeIndex][j][k], lineArray[i].g * se[destNodeIndex][j][k]));
								QExpr[i][j][k] = cplex.sum(QExpr[i][j][k], cplex.prod(fVar[srcNodeIndex][j][k],
										lineArray[i].b * -sf[destNodeIndex][j][k]));
								QExpr[i][j][k] = cplex.sum(QExpr[i][j][k], cplex.prod(eVar[srcNodeIndex][j][k],
										lineArray[i].b * -se[destNodeIndex][j][k]));
								QExpr[i][j][k] = cplex.sum(QExpr[i][j][k], cplex.prod(eVar[srcNodeIndex][j][k],
										lineArray[i].g * -sf[destNodeIndex][j][k]));
							} else if (destNodeIndex == id) {
								PExpr[i][j][k] = cplex.sum(PExpr[i][j][k],
										cplex.prod(lineArray[i].b * se[srcNodeIndex][j][k], eVar[destNodeIndex][j][k]));
								PExpr[i][j][k] = cplex.sum(PExpr[i][j][k], cplex
										.prod(lineArray[i].g * -se[srcNodeIndex][j][k], fVar[destNodeIndex][j][k]));
								PExpr[i][j][k] = cplex.sum(PExpr[i][j][k],
										cplex.prod(lineArray[i].g * sf[srcNodeIndex][j][k], eVar[destNodeIndex][j][k]));
								PExpr[i][j][k] = cplex.sum(PExpr[i][j][k],
										cplex.prod(lineArray[i].b * sf[srcNodeIndex][j][k], fVar[destNodeIndex][j][k]));

								QExpr[i][j][k] = cplex.sum(QExpr[i][j][k],
										cplex.prod(lineArray[i].g * sf[srcNodeIndex][j][k], eVar[destNodeIndex][j][k]));
								QExpr[i][j][k] = cplex.sum(QExpr[i][j][k], cplex
										.prod(lineArray[i].b * -sf[srcNodeIndex][j][k], fVar[destNodeIndex][j][k]));
								QExpr[i][j][k] = cplex.sum(QExpr[i][j][k], cplex
										.prod(lineArray[i].b * -se[srcNodeIndex][j][k], eVar[destNodeIndex][j][k]));
								QExpr[i][j][k] = cplex.sum(QExpr[i][j][k], cplex
										.prod(lineArray[i].g * -se[srcNodeIndex][j][k], fVar[destNodeIndex][j][k]));
							} else {
								PExpr[i][j][k] = cplex.sum(PExpr[i][j][k], sP[i][j][k]);
								QExpr[i][j][k] = cplex.sum(QExpr[i][j][k], sQ[i][j][k]);
								// cplex.addEq(eVar[srcNodeIndex][j][k], se[srcNodeIndex][j][k]);
								// cplex.addEq(eVar[destNodeIndex][j][k], se[destNodeIndex][j][k]);
								// cplex.addEq(fVar[srcNodeIndex][j][k], sf[srcNodeIndex][j][k]);
								// cplex.addEq(fVar[destNodeIndex][j][k], sf[destNodeIndex][j][k]);
							}
							cplex.addEq(pexprvar[i][j][k], PExpr[i][j][k]);
							cplex.addEq(qexprvar[i][j][k], QExpr[i][j][k]);

						}

						cplex.addEq(fexprvar[i][j][k],
								cplex.sum(cplex.prod(cplex.abs(pexprvar[i][j][k]), Math.abs(sP[i][j][k])),
										cplex.prod(cplex.abs(qexprvar[i][j][k]), Math.abs(sQ[i][j][k]))));

						busPExpr[srcNodeIndex][j][k] = cplex.sum(busPExpr[srcNodeIndex][j][k], pexprvar[i][j][k]);
						busPExpr[destNodeIndex][j][k] = cplex.diff(busPExpr[destNodeIndex][j][k], pexprvar[i][j][k]);

						lineExprp[i][j][k] = cplex.sum(-lineArray[i].constraint,
								cplex.sum(zLpVar[i][j][k], fexprvar[i][j][k]));

					}
				}
			}

			for (int i = 0; i < numOfBuses; i++) {
				for (int j = 0; j < numOfScenarios; j++) {
					for (int k = 0; k < numOfHours; k++) {
						if (iteration >= 10) {
							if (id != i) {
								cplex.addEq(pVar[i][j][k], sp[i][j][k]);
							}
						}
					}
				}
			}

			for (int i = 0; i < numOfScenarios; i++) {
				for (int j = 0; j < numOfHours; j++) {
					for (int k = 0; k < numOfBuses; k++) {
						if (iteration >= 9) {
							cplex.addLe(cplex.diff(eVar[k][i][j], se[k][i][j]), 0.05);
							cplex.addLe(-0.05, cplex.diff(eVar[k][i][j], se[k][i][j]));

							cplex.addLe(cplex.diff(fVar[k][i][j], sf[k][i][j]), 0.05);
							cplex.addLe(-0.05, cplex.diff(fVar[k][i][j], sf[k][i][j]));
						}
					}
				}
			}

			for (int b = 0; b < numOfBuses; b++) {
				for (int n = 0; n < numOfScenarios; n++) {
					if (transArray[initiallevel][n] > 0 && pbbArray[n][0] > 0) {
						cplex.addLe(pVar[b][n][0], 8);
						cplex.addLe(cplex.diff(4, pVar[b][n][0]), 4);
					}
				}
			}

			for (int t = 1; t < numOfHours; t++) {
				for (int b = 0; b < numOfBuses; b++) {
					for (int n = 0; n < numOfScenarios; n++) {
						for (int d = 0; d < numOfScenarios; d++) {
							if (transArray[d][n] > 0 && pbbArray[n][t] > 0 && pbbArray[d][t - 1] > 0) {
								cplex.addLe(pVar[b][n][t], cplex.sum(pVar[b][d][t - 1], 4));
								cplex.addLe(cplex.diff(pVar[b][d][t - 1], pVar[b][n][t]), 4);
							}
						}
					}
				}
			}

			IloNumExpr obj = cplex.numExpr();
			cplex.addLe(obj, 10000000);
			cplex.addLe(-10000000, obj);

			for (int j = 0; j < numOfScenarios; j++) {
				for (int k = 0; k < numOfHours; k++) {
					obj = cplex.sum(obj,
							cplex.prod(tap_changer_cost * 10000 * pbbArray[j][k], cplex.sum(bVar[j][k], cVar[j][k])));

					for (int i = 0; i < numOfBuses; i++) {
						obj = cplex.sum(obj, cplex.prod(busArray[i][k].cost * pbbArray[j][k], pVar[i][j][k]));
						obj = cplex.sum(obj, cplex.prod(-multB[i][j][k], busPExpr[i][j][k]));
						obj = cplex.sum(obj, cplex.prod(penalty, cplex.abs(busPExpr[i][j][k])));
					}

					for (int i = 0; i < numOfLines; i++) {
						obj = cplex.sum(obj, cplex.prod(multLp[i][j][k], lineExprp[i][j][k]));
						obj = cplex.sum(obj, cplex.prod(penalty, cplex.abs(lineExprp[i][j][k])));

						obj = cplex.sum(obj,
								cplex.prod(penalty, cplex.abs(cplex.diff(pexprvar[i][j][k], sP[i][j][k]))));
						obj = cplex.sum(obj,
								cplex.prod(penalty, cplex.abs(cplex.diff(qexprvar[i][j][k], sQ[i][j][k]))));
					}
					IloNumExpr tmp1 = cplex.numExpr();
					if (id == pvGenArray[0].busNum) {
						tmp1 = cplex.prod(eVar[0][j][k], pvGenArray[0].pvVol[j] * (a0 + sn[j][k] * delta_a) * 0.9);
					} else {
						tmp1 = cplex.sum(tmp1, se[pvGenArray[0].busNum][j][k] * pvGenArray[0].pvVol[j]
								* (a0 + sn[j][k] * delta_a) * 0.9);
					}

					IloNumExpr tmp2 = cplex.prod(0.1 * se[pvGenArray[0].busNum][j][k] * pvGenArray[0].pvVol[j],
							cplex.sum(a0, cplex.prod(nVar[j][k], delta_a)));
					IloNumExpr tmp3 = cplex.sum(tmp1, cplex.sum(-pvGenArray[0].pvGen[j], tmp2));
					IloNumExpr tmp4 = cplex.prod(ss,
							cplex.sum(a0, cplex.sum(cplex.prod(2 * delta_a, nVar[j][k]), -delta_a * sn[j][k])));
					IloNumExpr tmp5 = cplex.prod(tmp4, (a0 + delta_a * sn[j][k]) * pvGenArray[0].pvGen[j]);
					obj = cplex.sum(obj, cplex.prod(multT[j][k], cplex.sum(tmp3, tmp5)));
					obj = cplex.sum(obj, cplex.prod(penalty, cplex.abs(cplex.sum(tmp3, tmp5))));
				}
			}

			for (int i = 0; i < numOfScenarios; i++) {
				if (id == 0) {
					cplex.addEq(nVar[i][0], cplex.sum(3, cplex.diff(bVar[i][0], cVar[i][0])));
					cplex.addEq(nVar[i][1], cplex.sum(3, cplex.diff(bVar[i][1], cVar[i][1])));
				}
				for (int j = 0; j < numOfHours - 1; j++) {
					if (id == 0) {
						cplex.addEq(nVar[i][j + 1], cplex.sum(nVar[i][j], cplex.diff(bVar[i][j + 1], cVar[i][j + 1])));
					}
				}
			}

			for (int i = 0; i < numOfScenarios; i++) {
				for (int j = 0; j < numOfHours; j++) {
					cplex.addLe(bVar[i][j], cplex.prod(100, z1Var[i][j]));
					cplex.addLe(cVar[i][j], cplex.prod(100, cplex.diff(1, z1Var[i][j])));
				}
			}

			cplex.addMinimize(obj);
			cplex.setParam(IloCplex.LongParam.IntSolLim, 5);
			cplex.setParam(IloCplex.DoubleParam.EpGap, 0.00001);

			cplex.solve();

			// cplex.exportModel("lpex1.lp");

			double q = cplex.getObjValue();

			double[][][] eVal = new double[numOfBuses][numOfScenarios][numOfHours];
			double[][][] fVal = new double[numOfBuses][numOfScenarios][numOfHours];
			double[][][] pVal = new double[numOfBuses][numOfScenarios][numOfHours];
			for (int i = 0; i < numOfBuses; i++) {
				for (int j = 0; j < numOfScenarios; j++) {
					pVal[i][j] = cplex.getValues(pVar[i][j]);
				}
			}

			double[][][] szLp = new double[numOfLines][numOfScenarios][numOfHours];
			double[][][] szLn = new double[numOfLines][numOfScenarios][numOfHours];
			for (int i = 0; i < numOfLines; i++) {
				for (int j = 0; j < numOfScenarios; j++) {
					for (int k = 0; k < numOfHours; k++) {
						szLp[i][j][k] = cplex.getValue(zLpVar[i][j][k]);
						sP[i][j][k] = cplex.getValue(pexprvar[i][j][k]);
						sQ[i][j][k] = cplex.getValue(qexprvar[i][j][k]);
						sF[i][j][k] = cplex.getValue(fexprvar[i][j][k]);
					}
				}
			}

			sn = new Integer[numOfScenarios][numOfHours];
			sb = new Integer[numOfScenarios][numOfHours];
			sc = new Integer[numOfScenarios][numOfHours];

			double[][] nValDoubles = new double[numOfScenarios][numOfHours];
			double[][] bValDoubles = new double[numOfScenarios][numOfHours];
			double[][] cValDoubles = new double[numOfScenarios][numOfHours];
			for (int k = 0; k < numOfScenarios; k++) {
				nValDoubles[k] = cplex.getValues(nVar[k]);
				bValDoubles[k] = cplex.getValues(bVar[k]);
				cValDoubles[k] = cplex.getValues(cVar[k]);
			}
			for (int i = 0; i < numOfScenarios; i++) {
				for (int j = 0; j < numOfHours; j++) {
					for (int k = 0; k < numOfBuses; k++) {
						eVal[k][i][j] = cplex.getValue(eVar[k][i][j]);
						fVal[k][i][j] = cplex.getValue(fVar[k][i][j]);
					}
					sn[i][j] = (int) Math.round(nValDoubles[i][j]);
					sb[i][j] = (int) Math.round(bValDoubles[i][j]);
					sc[i][j] = (int) Math.round(cValDoubles[i][j]);
				}
			}

			// System.out.println("pexprtest: "+Arrays.deepToString(pExpr));

			cplex.end();

			collector.emit(new Values(q, sP, sQ, sF, eVal, fVal, pVal, szLp, szLn, sn, sb, sc, id, iteration));

		} catch (IloException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void prepare(Map arg0, TopologyContext arg1) {
		// TODO Auto-generated method stub
	}

}
