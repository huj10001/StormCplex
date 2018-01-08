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
import ilog.concert.IloIntVar;
import ilog.concert.IloNumExpr;
import ilog.concert.IloNumVar;
import ilog.cplex.IloCplex;

public class SubproblemSolverBolt_update implements IBasicBolt {
	IloCplex cplex;
	public Integer numOfBuses;
	public Integer numOfLines;
	public Integer numOfScenarios;
	public double[][] se, sf, sp, szLp, szLn;
	public double[][] multB, multLp, multLn;
	public double[] PVgen;
	public Integer iteration;

	double Lagrangian = -1;

	double step = 3.5 / 10;
	double oldstep = 3.5 / 10;

	double norm = 100.0;
	double oldnorm = 100.0;

	double penalty = 1 / 100000;

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("q", "p12", "p13", "p32", "eVal", "fVal", "pVal", "szLp", "szLn", "id"));
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
		multB = (double[][]) input.getValueByField("multB");
		multLn = (double[][]) input.getValueByField("multLn");
		multLp = (double[][]) input.getValueByField("multLp");
		se = (double[][]) input.getValueByField("se");
		sf = (double[][]) input.getValueByField("sf");
		sp = (double[][]) input.getValueByField("sp");
		szLp = (double[][]) input.getValueByField("szLp");
		szLn = (double[][]) input.getValueByField("szLn");
		PVgen = (double[]) input.getValueByField("PVgen");
		iteration = input.getIntegerByField("iteration");
		try {
			cplex = new IloCplex();
			cplex.setOut(null);

			IloNumVar[][] eVar = new IloNumVar[numOfBuses][numOfScenarios];
			IloNumVar[][] fVar = new IloNumVar[numOfBuses][numOfScenarios];
			IloNumVar[][] pVar = new IloNumVar[numOfBuses][numOfScenarios];
			IloIntVar[][] xVar = new IloIntVar[numOfBuses][numOfScenarios];
			IloNumVar[][] zLpVar = new IloNumVar[numOfLines][numOfScenarios];
			IloNumVar[][] zLnVar = new IloNumVar[numOfLines][numOfScenarios];
			for (int i = 0; i < numOfBuses; i++) {
				for (int j = 0; j < numOfScenarios; j++) {
					eVar[i][j] = cplex.numVar(-1.5, 1.5);
					cplex.add(eVar[i][j]);
					fVar[i][j] = cplex.numVar(-1.5, 1.5);
					cplex.add(fVar[i][j]);
					pVar[i][j] = cplex.numVar(2, 20);
					cplex.add(pVar[i][j]);
					xVar[i][j] = cplex.intVar(0, 1);
					cplex.add(xVar[i][j]);
				}
			}

			for (int i = 0; i < numOfLines; i++) {
				for (int j = 0; j < numOfScenarios; j++) {
					zLpVar[i][j] = cplex.numVar(0, 100);
					cplex.add(zLpVar[i][j]);
					zLnVar[i][j] = cplex.numVar(0, 100);
					cplex.add(zLnVar[i][j]);
				}
			}

			IloNumExpr[] P12 = new IloNumExpr[numOfScenarios];
			IloNumExpr[] P13 = new IloNumExpr[numOfScenarios];
			IloNumExpr[] P32 = new IloNumExpr[numOfScenarios];
			for (int i = 0; i < numOfScenarios; i++) {
				P12[i] = cplex.numExpr();
				P13[i] = cplex.numExpr();
				P32[i] = cplex.numExpr();
			}

			for (int i = 0; i < numOfScenarios; i++) {
				if (id == 0) {
					P12[i] = cplex.sum(P12[i], cplex.prod(eVar[0][i], se[1][i] - sf[1][i]));
					P12[i] = cplex.sum(P12[i], cplex.prod(fVar[0][i], se[1][i] + sf[1][i]));
					P13[i] = cplex.sum(P13[i], cplex.prod(eVar[0][i], se[2][i] - sf[2][i]));
					P13[i] = cplex.sum(P13[i], cplex.prod(fVar[0][i], se[2][i] + sf[2][i]));
					P32[i] = cplex.sum(P32[i], se[2][i] * (se[1][i] - sf[1][i]));
					P32[i] = cplex.sum(P32[i], sf[2][i] * (se[1][i] + sf[1][i]));
					if (iteration > 10) {
						cplex.addEq(pVar[1][i], sp[1][i]);
						cplex.addEq(pVar[2][i], sp[2][i]);
					}
				} else if (id == 1) {
					P12[i] = cplex.sum(P12[i], cplex.prod(eVar[1][i], se[0][i] + sf[0][i]));
					P12[i] = cplex.sum(P12[i], cplex.prod(fVar[1][i], sf[0][i] - se[0][i]));
					P13[i] = cplex.sum(P13[i], se[0][i] * (se[2][i] - sf[2][i]));
					P13[i] = cplex.sum(P13[i], sf[0][i] * (se[2][i] + sf[2][i]));
					P32[i] = cplex.sum(P32[i], cplex.prod(eVar[1][i], se[2][i] + sf[2][i]));
					P32[i] = cplex.sum(P32[i], cplex.prod(fVar[1][i], sf[2][i] - se[2][i]));
					if (iteration > 10) {
						cplex.addEq(pVar[0][i], sp[0][i]);
						cplex.addEq(pVar[2][i], sp[2][i]);
					}
				} else if (id == 2) {
					P12[i] = cplex.sum(P12[i], se[0][i] * (se[1][i] - sf[1][i]));
					P12[i] = cplex.sum(P12[i], sf[0][i] * (se[1][i] + sf[1][i]));
					P13[i] = cplex.sum(P13[i], cplex.prod(eVar[2][i], se[0][i] + sf[0][i]));
					P13[i] = cplex.sum(P13[i], cplex.prod(fVar[2][i], sf[0][i] + se[0][i]));
					P32[i] = cplex.sum(P32[i], cplex.prod(eVar[2][i], se[1][i] - sf[1][i]));
					P32[i] = cplex.sum(P32[i], cplex.prod(fVar[2][i], se[1][i] + sf[1][i]));
					if (iteration > 10) {
						cplex.addEq(pVar[1][i], sp[1][i]);
						cplex.addEq(pVar[0][i], sp[0][i]);
					}
				}

				for (int j = 0; j < numOfBuses; j++) {
					cplex.addLe(pVar[j][i], cplex.prod(20, xVar[j][i]));
					cplex.addLe(cplex.prod(2, xVar[j][i]), pVar[j][i]);
				}

				cplex.addLe(P12[i], 200);
				cplex.addLe(-200, P12[i]);
				cplex.addLe(P13[i], 200);
				cplex.addLe(-200, P13[i]);
				cplex.addLe(P32[i], 200);
				cplex.addLe(-200, P32[i]);
			}

			IloNumExpr obj = cplex.numExpr();
			cplex.addLe(obj, 10000);
			cplex.addLe(-10000, obj);
			for (int i = 0; i < numOfScenarios; i++) {
				IloNumExpr first = cplex.numExpr();
				first = cplex.sum(first, cplex.prod(pVar[0][i], 30));
				first = cplex.sum(first, cplex.prod(pVar[1][i], 2));
				first = cplex.sum(first, pVar[2][i]);
				obj = cplex.sum(obj, cplex.prod(first, 0.2));

				obj = cplex.sum(obj, cplex.prod(multB[0][i],
						cplex.sum(P12[i], cplex.sum(pVar[0][i], cplex.sum(cplex.negative(P13[i]), PVgen[i] - 10)))));
				obj = cplex.sum(obj, cplex.prod(multB[1][i],
						cplex.sum(P32[i], cplex.sum(pVar[1][i], cplex.sum(cplex.negative(P12[i]), -5)))));
				obj = cplex.sum(obj, cplex.prod(multB[2][i],
						cplex.sum(P13[i], cplex.sum(pVar[2][i], cplex.sum(cplex.negative(P32[i]), -5)))));

				obj = cplex.sum(obj, cplex.prod(multLp[0][i], cplex.sum(P12[i], cplex.sum(-1.5, zLpVar[0][i]))));
				obj = cplex.sum(obj, cplex.prod(multLp[1][i], cplex.sum(P13[i], cplex.sum(-1.5, zLpVar[1][i]))));
				obj = cplex.sum(obj, cplex.prod(multLp[2][i], cplex.sum(P32[i], cplex.sum(-1.5, zLpVar[2][i]))));

				obj = cplex.sum(obj,
						cplex.prod(multLn[0][i], cplex.sum(cplex.negative(P12[i]), cplex.sum(-1.5, zLnVar[0][i]))));
				obj = cplex.sum(obj,
						cplex.prod(multLn[1][i], cplex.sum(cplex.negative(P13[i]), cplex.sum(-1.5, zLnVar[1][i]))));
				obj = cplex.sum(obj,
						cplex.prod(multLn[2][i], cplex.sum(cplex.negative(P32[i]), cplex.sum(-1.5, zLnVar[2][i]))));
			}
			cplex.addMinimize(obj);
			////////////////////////////////

			cplex.solve();
			// cplex.exportModel("lpex1.lp");
			double q = cplex.getObjValue();

			double[] p12 = new double[numOfScenarios];
			double[] p13 = new double[numOfScenarios];
			double[] p32 = new double[numOfScenarios];

			double[][] eVal = new double[numOfBuses][numOfScenarios];
			double[][] fVal = new double[numOfBuses][numOfScenarios];
			double[][] pVal = new double[numOfBuses][numOfScenarios];

			for (int i = 0; i < numOfScenarios; i++) {
				p12[i] = cplex.getValue(P12[i]);
				p13[i] = cplex.getValue(P13[i]);
				p32[i] = cplex.getValue(P32[i]);
			}
			for (int i = 0; i < numOfBuses; i++) {
				eVal[i] = cplex.getValues(eVar[i]);
				fVal[i] = cplex.getValues(fVar[i]);
				pVal[i] = cplex.getValues(pVar[i]);
			}

			double[][] szLp = new double[numOfLines][numOfScenarios];
			double[][] szLn = new double[numOfLines][numOfScenarios];

			for (int i = 0; i < numOfLines; i++) {
				szLp[i] = cplex.getValues(zLpVar[i]);
				szLn[i] = cplex.getValues(zLnVar[i]);
			}
			cplex.end();

			collector.emit(new Values(q, p12, p13, p32, eVal, fVal, pVal, szLp, szLn, id));
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
