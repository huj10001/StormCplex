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
import ilog.concert.IloIntVar;
import ilog.concert.IloNumExpr;
import ilog.concert.IloNumVar;
import ilog.cplex.IloCplex;

public class SubproblemSolverBolt implements IBasicBolt {
	IloCplex cplex;
	public Integer numOfBuses;
	public Integer numOfLines;
	public Integer numOfScenarios;
	public double[][] se, sf, sp;
	public Integer[] sn, sb, sc;
	public double[][] multB, multLp, multLn;
	public double[] PVgen, multT, busMult, busP;
	public Integer iteration;
	public Map<Line, Double> lineMap = new HashMap<>();
	public Map<Line, double[]> pExprMap;
	public Map<Line, double[]> qExprMap;
	public Map<Line, double[]> fExprMap;
	public double penalty;
	double a0 = 1d;
	double delta_a = 0.1;
	double ss = 0.75;

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("q", "pExprMap", "qExprMap", "fExprMap", "eVal", "fVal", "pVal", "szLp", "szLn",
				"sn", "sb", "sc", "id", "lastIteartion"));
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
		multT = (double[]) input.getValueByField("multT");
		multB = (double[][]) input.getValueByField("multB");
		multLn = (double[][]) input.getValueByField("multLn");
		multLp = (double[][]) input.getValueByField("multLp");
		se = (double[][]) input.getValueByField("se");
		sf = (double[][]) input.getValueByField("sf");
		sp = (double[][]) input.getValueByField("sp");
		sn = (Integer[]) input.getValueByField("sn");
		sb = (Integer[]) input.getValueByField("sb");
		sc = (Integer[]) input.getValueByField("sc");
		PVgen = (double[]) input.getValueByField("PVgen");
		busMult = (double[]) input.getValueByField("busMult");
		busP = (double[]) input.getValueByField("busP");
		lineMap = (Map<Line, Double>) input.getValueByField("lineMap");
		penalty = (double) input.getValueByField("penalty");
		iteration = input.getIntegerByField("iteration");
		pExprMap = (Map<Line, double[]>) input.getValueByField("pExprMap");
		qExprMap = (Map<Line, double[]>) input.getValueByField("qExprMap");
		fExprMap = (Map<Line, double[]>) input.getValueByField("fExprMap");

		try {
			cplex = new IloCplex();
			cplex.setOut(null);

			IloNumVar[] eVar = new IloNumVar[numOfScenarios];
			IloNumVar[] fVar = new IloNumVar[numOfScenarios];
			IloNumVar[][] pVar = new IloNumVar[numOfBuses][numOfScenarios];
			IloNumVar[][] zLpVar = new IloNumVar[numOfLines][numOfScenarios];
			// IloNumVar[][] zLnVar = new IloNumVar[numOfLines][numOfScenarios];

			for (int i = 0; i < numOfBuses; i++) {
				for (int j = 0; j < numOfScenarios; j++) {
					pVar[i][j] = cplex.numVar(2, 50, "p" + i + j);
					cplex.add(pVar[i][j]);
				}
			}

			for (int i = 0; i < numOfLines; i++) {
				for (int j = 0; j < numOfScenarios; j++) {
					zLpVar[i][j] = cplex.numVar(0, 10, "zlp"+i+j);
					cplex.add(zLpVar[i][j]);
					// zLnVar[i][j] = cplex.numVar(0, 10);
					// cplex.add(zLnVar[i][j]);
				}
			}

			IloIntVar[] nVar = new IloIntVar[numOfScenarios];
			IloIntVar[] bVar = new IloIntVar[numOfScenarios];
			IloIntVar[] cVar = new IloIntVar[numOfScenarios];
			IloIntVar[] z1Var = new IloIntVar[numOfScenarios];

			for (int i = 0; i < numOfScenarios; i++) {
				eVar[i] = cplex.numVar(-2.5, 2.5, "e"+i);
				cplex.add(eVar[i]);
				fVar[i] = cplex.numVar(-2.5, 2.5, "f"+i);
				cplex.add(fVar[i]);

				nVar[i] = cplex.intVar(0, 5, "n"+i);
				cplex.add(nVar[i]);
				bVar[i] = cplex.intVar(0, 10, "b"+i);
				cplex.add(bVar[i]);
				cVar[i] = cplex.intVar(0, 10, "c"+i);
				cplex.add(cVar[i]);
				z1Var[i] = cplex.boolVar("z1"+i);
				cplex.add(z1Var[i]);
			}

			IloNumExpr[][] busPExpr = new IloNumExpr[numOfBuses][numOfScenarios];
			for (int i = 0; i < numOfBuses; i++) {
				for (int j = 0; j < numOfScenarios; j++) {
					busPExpr[i][j] = cplex.numExpr();
					busPExpr[i][j] = cplex.sum(busPExpr[i][j], -busP[i]);
					busPExpr[i][j] = cplex.sum(busPExpr[i][j], pVar[i][j]);
				}
			}

			for (int j = 0; j < numOfScenarios; j++) {
				busPExpr[0][j] = cplex.sum(busPExpr[0][j], PVgen[j]);
			}

			IloNumExpr[][] PExpr = new IloNumExpr[numOfLines][numOfScenarios];
			IloNumExpr[][] QExpr = new IloNumExpr[numOfLines][numOfScenarios];
			IloNumExpr[][] FExpr = new IloNumExpr[numOfLines][numOfScenarios];
			IloNumExpr[][] lineExprp = new IloNumExpr[numOfLines][numOfScenarios];
			IloNumExpr[][] lineExprn = new IloNumExpr[numOfLines][numOfScenarios];
			Line[] lineList = new Line[numOfLines];
			double[] doubleList = new double[numOfLines];
			int k = 0;
			for (Entry<Line, Double> entry : lineMap.entrySet()) {
				lineList[k] = entry.getKey();
				doubleList[k] = entry.getValue();
				k++;
			}

			for (int i = 0; i < numOfScenarios; i++) {
				if (iteration >= 10) {
					if (id != 0) {
						cplex.addEq(nVar[i], sn[i]);
						cplex.addEq(bVar[i], sb[i]);
						cplex.addEq(cVar[i], sc[i]);
					}
				}
			}

			IloNumVar[][] pexprvar = new IloNumVar[numOfLines][numOfScenarios];
			IloNumVar[][] qexprvar = new IloNumVar[numOfLines][numOfScenarios];
			IloNumVar[][] fexprvar = new IloNumVar[numOfLines][numOfScenarios];
			for (int m = 0; m < numOfLines; m++) {
				for (int n = 0; n < numOfScenarios; n++) {
					pexprvar[m][n] = cplex.numVar(-20, 20, "P"+m+n);
					cplex.add(pexprvar[m][n]);
					qexprvar[m][n] = cplex.numVar(-20, 20, "Q"+m+n);
					cplex.add(qexprvar[m][n]);
					fexprvar[m][n] = cplex.numVar(-20000, 20000, "F"+m+n);
					cplex.add(fexprvar[m][n]);
				}
			}

			for (int i = 0; i < numOfLines; i++) {
				for (int j = 0; j < numOfScenarios; j++) {
					int srcNodeIndex = lineList[i].i;
					int destNodeIndex = lineList[i].j;
					PExpr[i][j] = cplex.numExpr();
					QExpr[i][j] = cplex.numExpr();
					FExpr[i][j] = cplex.numExpr();
					lineExprp[i][j] = cplex.numExpr();
					lineExprn[i][j] = cplex.numExpr();
					Line newline = new Line(srcNodeIndex, destNodeIndex);
//					if (iteration >= 10) {
//						cplex.addEq(PExpr[i][j], pexprvar[i][j]);
//						cplex.addEq(QExpr[i][j], qexprvar[i][j]);
//						cplex.addEq(FExpr[i][j], fexprvar[i][j]);
//					}

					if (iteration >= 10) {
						if (srcNodeIndex == id) {
							PExpr[i][j] = cplex.sum(PExpr[i][j], cplex.prod(eVar[j], se[destNodeIndex][j]));
							PExpr[i][j] = cplex.sum(PExpr[i][j], cplex.prod(eVar[j], -sf[destNodeIndex][j]));
							PExpr[i][j] = cplex.sum(PExpr[i][j], cplex.prod(fVar[j], se[destNodeIndex][j]));
							PExpr[i][j] = cplex.sum(PExpr[i][j], cplex.prod(fVar[j], sf[destNodeIndex][j]));

							QExpr[i][j] = cplex.sum(QExpr[i][j], cplex.prod(fVar[j], se[destNodeIndex][j]));
							QExpr[i][j] = cplex.sum(QExpr[i][j], cplex.prod(fVar[j], -sf[destNodeIndex][j]));
							QExpr[i][j] = cplex.sum(QExpr[i][j], cplex.prod(eVar[j], -se[destNodeIndex][j]));
							QExpr[i][j] = cplex.sum(QExpr[i][j], cplex.prod(eVar[j], -sf[destNodeIndex][j]));
						} else if (destNodeIndex == id) {
							PExpr[i][j] = cplex.sum(PExpr[i][j], cplex.prod(se[srcNodeIndex][j], eVar[j]));
							PExpr[i][j] = cplex.sum(PExpr[i][j], cplex.prod(-se[srcNodeIndex][j], fVar[j]));
							PExpr[i][j] = cplex.sum(PExpr[i][j], cplex.prod(sf[srcNodeIndex][j], eVar[j]));
							PExpr[i][j] = cplex.sum(PExpr[i][j], cplex.prod(sf[srcNodeIndex][j], fVar[j]));

							QExpr[i][j] = cplex.sum(QExpr[i][j], cplex.prod(sf[srcNodeIndex][j], eVar[j]));
							QExpr[i][j] = cplex.sum(QExpr[i][j], cplex.prod(-sf[srcNodeIndex][j], fVar[j]));
							QExpr[i][j] = cplex.sum(QExpr[i][j], cplex.prod(-se[srcNodeIndex][j], eVar[j]));
							QExpr[i][j] = cplex.sum(QExpr[i][j], cplex.prod(-se[srcNodeIndex][j], fVar[j]));
						} else {
							PExpr[i][j] = cplex.sum(PExpr[i][j], se[srcNodeIndex][j] * se[destNodeIndex][j]);
							PExpr[i][j] = cplex.sum(PExpr[i][j], -se[srcNodeIndex][j] * sf[destNodeIndex][j]);
							PExpr[i][j] = cplex.sum(PExpr[i][j], sf[srcNodeIndex][j] * se[destNodeIndex][j]);
							PExpr[i][j] = cplex.sum(PExpr[i][j], sf[srcNodeIndex][j] * sf[destNodeIndex][j]);

							QExpr[i][j] = cplex.sum(QExpr[i][j], sf[srcNodeIndex][j] * se[destNodeIndex][j]);
							QExpr[i][j] = cplex.sum(QExpr[i][j], -sf[srcNodeIndex][j] * sf[destNodeIndex][j]);
							QExpr[i][j] = cplex.sum(QExpr[i][j], -se[srcNodeIndex][j] * se[destNodeIndex][j]);
							QExpr[i][j] = cplex.sum(QExpr[i][j], -se[srcNodeIndex][j] * sf[destNodeIndex][j]);
						}
//						System.out.println("se: "+ Double.toString(se[srcNodeIndex][j]) +", sf: "+ Double.toString(sf[destNodeIndex][j]));
//						cplex.addEq(pexprvar[i][j],PExpr[i][j]);
//						cplex.addEq(qexprvar[i][j],QExpr[i][j]);
						cplex.addEq(PExpr[i][j],pexprvar[i][j]);
						cplex.addEq(QExpr[i][j],qexprvar[i][j]);
//						cplex.addEq(FExpr[i][j],fexprvar[i][j]);
					}
					System.out.println(srcNodeIndex+" "+j+" "+"se: "+ Double.toString(se[srcNodeIndex][j]) +", sf: "+ Double.toString(sf[srcNodeIndex][j]));

					// cplex.addLe(PExpr[i][j], 20);
					// cplex.addLe(-20, PExpr[i][j]);
					// cplex.addLe(QExpr[i][j], 20);
					// cplex.addLe(-20, QExpr[i][j]);
					// cplex.addLe(FExpr[i][j], 20000);
					// cplex.addLe(-20000, FExpr[i][j]);
					
					cplex.addEq(fexprvar[i][j], cplex.sum(cplex.prod(cplex.abs(pexprvar[i][j]), Math.abs(pExprMap.get(newline)[j])),
							cplex.prod(cplex.abs(qexprvar[i][j]), Math.abs(qExprMap.get(newline)[j]))));
					
					busPExpr[srcNodeIndex][j] = cplex.sum(busPExpr[srcNodeIndex][j], pexprvar[i][j]);
					busPExpr[destNodeIndex][j] = cplex.diff(busPExpr[destNodeIndex][j], pexprvar[i][j]);
					lineExprp[i][j] = cplex.sum(lineExprp[i][j],
							cplex.sum(-doubleList[i], cplex.sum(zLpVar[i][j], fexprvar[i][j])));
					// lineExprn[i][j] = cplex.sum(lineExprn[i][j],
					// cplex.sum(-doubleList[i], cplex.sum(zLnVar[i][j], FExpr[i][j])));
				}
			}

			for (int i = 0; i < numOfBuses; i++) {
				for (int j = 0; j < numOfScenarios; j++) {
					if (iteration > 10) {
						if (id != i) {
							cplex.addEq(pVar[i][j], sp[i][j]);
						}
					}
				}
			}

			for (int i = 0; i < numOfScenarios; i++) {
				if (iteration >= 10) {
					cplex.addLe(cplex.diff(eVar[i], se[id][i]), 0.1);
					cplex.addLe(-0.1, cplex.diff(eVar[i], se[id][i]));

					cplex.addLe(cplex.diff(fVar[i], sf[id][i]), 0.1);
					cplex.addLe(-0.1, cplex.diff(fVar[i], sf[id][i]));
				}
			}

			IloNumExpr obj = cplex.numExpr();
			cplex.addLe(obj, 10000000);
			cplex.addLe(-10000000, obj);

			for (int j = 0; j < numOfScenarios; j++) {

				obj = cplex.sum(obj, cplex.prod(200, cplex.sum(bVar[j], cVar[j])));

				for (int i = 0; i < numOfBuses; i++) {
					obj = cplex.sum(obj, cplex.prod(busMult[i] * 0.2, pVar[i][j]));
					obj = cplex.sum(obj, cplex.prod(multB[i][j], busPExpr[i][j]));
					obj = cplex.sum(obj, cplex.prod(penalty, cplex.abs(busPExpr[i][j])));
				}

				for (int i = 0; i < numOfLines; i++) {
					obj = cplex.sum(obj, cplex.prod(multLp[i][j], lineExprp[i][j]));
					// obj = cplex.sum(obj, cplex.prod(multLn[i][j], lineExprn[i][j]));
					obj = cplex.sum(obj, cplex.prod(penalty, cplex.abs(lineExprp[i][j])));
					// obj = cplex.sum(obj, cplex.prod(penalty, cplex.abs(lineExprn[i][j])));

//					int srcNodeIndex = lineList[i].i;
//					int destNodeIndex = lineList[i].j;
//					Line newline = new Line(srcNodeIndex, destNodeIndex);
//					obj = cplex.prod(penalty, cplex.abs(cplex.diff(pexprvar[i][j], pExprMap.get(newline)[j])));
//					obj = cplex.prod(penalty, cplex.abs(cplex.diff(qexprvar[i][j], qExprMap.get(newline)[j])));
				}
				IloNumExpr tmp1 = cplex.numExpr();
				if (id == 0) {
					tmp1 = cplex.prod(eVar[j], Math.sqrt(PVgen[j]) * (a0 + sn[j] * delta_a) * 0.9);
				} else {
					tmp1 = cplex.sum(tmp1, se[0][j] * Math.sqrt(PVgen[j]) * (a0 + sn[j] * delta_a) * 0.9);
				}

				IloNumExpr tmp2 = cplex.prod(0.1 * se[0][j] * Math.sqrt(PVgen[j]),
						cplex.sum(a0, cplex.prod(nVar[j], delta_a)));
				IloNumExpr tmp3 = cplex.sum(tmp1, cplex.sum(-PVgen[j], tmp2));
				IloNumExpr tmp4 = cplex.prod(ss,
						cplex.sum(a0, cplex.sum(cplex.prod(2 * delta_a, nVar[j]), -delta_a * sn[j])));
				IloNumExpr tmp5 = cplex.prod(tmp4, (a0 + delta_a * sn[j]) * PVgen[j]);
				obj = cplex.sum(obj, cplex.prod(multT[j], cplex.sum(tmp3, tmp5)));
				obj = cplex.sum(obj, cplex.prod(penalty, cplex.abs(cplex.sum(tmp3, tmp5))));
			}

			for (int i = 0; i < numOfScenarios; i++) {
				if (id == 0) {
					cplex.addEq(nVar[i], cplex.sum(3, cplex.diff(bVar[i], cVar[i])));
				}
				cplex.addLe(bVar[i], cplex.prod(100, z1Var[i]));
				cplex.addLe(cVar[i], cplex.prod(100, cplex.diff(1, z1Var[i])));
			}

			cplex.addMinimize(obj);
			// cplex.setParam(IloCplex.IntParam.FracCuts, 2);
			cplex.setParam(IloCplex.LongParam.IntSolLim, 5);
			cplex.setParam(IloCplex.DoubleParam.EpGap, 0.00001);
			////////////////////////////////

			cplex.solve();

			cplex.exportModel("lpex1.lp");
			double q = cplex.getObjValue();

			double[] eVal = new double[numOfScenarios];
			double[] fVal = new double[numOfScenarios];
			double[][] pVal = new double[numOfBuses][numOfScenarios];
			for (int i = 0; i < numOfBuses; i++) {

				pVal[i] = cplex.getValues(pVar[i]);
			}

			double[][] szLp = new double[numOfLines][numOfScenarios];
			double[][] szLn = new double[numOfLines][numOfScenarios];
			double[][] pExpr = new double[numOfLines][numOfScenarios];
			double[][] qExpr = new double[numOfLines][numOfScenarios];
			double[][] fExpr = new double[numOfLines][numOfScenarios];
			for (int i = 0; i < numOfLines; i++) {
				szLp[i] = cplex.getValues(zLpVar[i]);
				// szLn[i] = cplex.getValues(zLnVar[i]);
				for (int j = 0; j < numOfScenarios; j++) {
					pExpr[i][j] = cplex.getValue(pexprvar[i][j]);
					qExpr[i][j] = cplex.getValue(qexprvar[i][j]);
					fExpr[i][j] = cplex.getValue(fexprvar[i][j]);
				}
			}

			sn = new Integer[numOfScenarios];
			// Arrays.fill(sn, 1);
			sb = new Integer[numOfScenarios];
			// Arrays.fill(sb, 1);
			sc = new Integer[numOfScenarios];
			// Arrays.fill(sc, 1);

			double[] nValDoubles = cplex.getValues(nVar);
			double[] bValDoubles = cplex.getValues(bVar);
			double[] cValDoubles = cplex.getValues(cVar);
			for (int i = 0; i < numOfScenarios; i++) {
				eVal[i] = cplex.getValue(eVar[i]);
				fVal[i] = cplex.getValue(fVar[i]);
				sn[i] = (int) Math.round(nValDoubles[i]);
				sb[i] = (int) Math.round(bValDoubles[i]);
				sc[i] = (int) Math.round(cValDoubles[i]);
			}

			pExprMap = new HashMap<>();
			qExprMap = new HashMap<>();
			fExprMap = new HashMap<>();
			for (int i = 0; i < numOfLines; i++) {
				pExprMap.put(lineList[i], pExpr[i]);
				qExprMap.put(lineList[i], qExpr[i]);
				fExprMap.put(lineList[i], fExpr[i]);
			}

			cplex.end();

			collector.emit(new Values(q, pExprMap, qExprMap, fExprMap, eVal, fVal, pVal, szLp, szLn, sn, sb, sc, id,
					iteration));
			
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
