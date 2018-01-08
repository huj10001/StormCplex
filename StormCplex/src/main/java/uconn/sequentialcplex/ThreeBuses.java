package uconn.sequentialcplex;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import ilog.concert.IloException;
import ilog.concert.IloIntVar;
import ilog.concert.IloNumExpr;
import ilog.concert.IloNumVar;
import ilog.cplex.IloCplex;

public class ThreeBuses {
	public static final String FileName = "dataThreeBuses";

	public static void main(String[] args) {

		try {
			// Create the modeler/solver object
			IloCplex cplex = new IloCplex();

			long iter_limit = 120000;

			int numOfBuses = 0;
			int numOfLines = 0;

			double Lagrangian = -1;

			double step = 3.5 / 10;
			double oldstep = 3.5 / 10;

			double norm = 100.0;
			double oldnorm = 100.0;

			double penalty = 1 / 100000;

			try (BufferedReader br = Files.newBufferedReader(Paths.get(FileName))) {
				String[] problemSizeString = br.readLine().trim().split("\\s+");

				// number of machines
				numOfBuses = Integer.parseInt(problemSizeString[0]);
				System.out.println("Number of Buses:" + numOfBuses);

				// number of jobs
				numOfLines = Integer.parseInt(problemSizeString[1]);

				double[] multB = new double[numOfBuses];
				for (int i = 0; i < numOfBuses; i++)
					multB[i] = 0;
				double[] multV = new double[numOfBuses];
				for (int i = 0; i < numOfBuses; i++)
					multV[i] = 0;
				double[] multLn = new double[numOfBuses];
				for (int i = 0; i < numOfBuses; i++)
					multLn[i] = 0;
				double[] multLp = new double[numOfBuses];
				for (int i = 0; i < numOfBuses; i++)
					multLp[i] = 0;
				double[] multDC = new double[numOfBuses];
				for (int i = 0; i < numOfBuses; i++)
					multDC[i] = 0;
				double[] multP = new double[numOfBuses];
				for (int i = 0; i < numOfBuses; i++)
					multP[i] = 0;

				double[] slackB = new double[numOfBuses];
				for (int i = 0; i < numOfBuses; i++)
					slackB[i] = 0;
				double[] slackV = new double[numOfBuses];
				for (int i = 0; i < numOfBuses; i++)
					slackV[i] = 0;
				double[] slackLn = new double[numOfBuses];
				for (int i = 0; i < numOfBuses; i++)
					slackLn[i] = 0;
				double[] slackLp = new double[numOfBuses];
				for (int i = 0; i < numOfBuses; i++)
					slackLp[i] = 0;
				double[] slackDC = new double[numOfBuses];
				for (int i = 0; i < numOfBuses; i++)
					slackDC[i] = 0;
				double[] slackP = new double[numOfBuses];
				for (int i = 0; i < numOfBuses; i++)
					slackP[i] = 0;

				double[] se = new double[numOfBuses];
				String[] seStr = br.readLine().trim().split("\\s+");
				for (int i = 0; i < numOfBuses; i++) {
					se[i] = Float.parseFloat(seStr[i]);
					System.out.print(se[i] + " ");
				}
				System.out.println();

				double[] sf = new double[numOfBuses];
				String[] sfStr = br.readLine().trim().split("\\s+");
				for (int i = 0; i < numOfBuses; i++) {
					sf[i] = Float.parseFloat(sfStr[i]);
				}

				double[] sp = new double[numOfBuses];
				String[] spStr = br.readLine().trim().split("\\s+");
				for (int i = 0; i < numOfBuses; i++) {
					sp[i] = Float.parseFloat(spStr[i]);
				}

				double[] szLp = new double[numOfLines];
				for (int i = 0; i < numOfLines; i++)
					szLp[i] = 0;
				double[] szLn = new double[numOfLines];
				for (int i = 0; i < numOfLines; i++)
					szLn[i] = 0;

				double adj12 = 0;
				String[] adjTmp = br.readLine().trim().split("\\s+");
				adj12 = Float.parseFloat(adjTmp[0]);
				double adj13 = 0;
				adjTmp = br.readLine().trim().split("\\s+");
				adj13 = Float.parseFloat(adjTmp[0]);
				double adj32 = 0;
				adjTmp = br.readLine().trim().split("\\s+");
				adj32 = Float.parseFloat(adjTmp[0]);

				double[][] SF = new double[numOfBuses][numOfLines];
				// set the number of machines
				for (int i = 0; i < numOfBuses; i++) {
					String[] SFTmp = br.readLine().trim().split("\\s+");
					for (int j = 0; j < numOfLines; j++) {
						SF[i][j] = Double.parseDouble(SFTmp[j]);
					}
				}

				for (int k = 0; k < iter_limit; k++) {

					cplex = new IloCplex();
					cplex.setOut(null);

					IloNumVar[] eVar = new IloNumVar[numOfBuses];
					for (int i = 0; i < numOfBuses; i++) {
						eVar[i] = cplex.numVar(-1.5, 1.5, "e" + i);
						cplex.add(eVar[i]);
					}
					IloNumVar[] fVar = new IloNumVar[numOfBuses];
					for (int i = 0; i < numOfBuses; i++) {
						fVar[i] = cplex.numVar(-1.5, 1.5, "f" + i);
						cplex.add(fVar[i]);
					}
					IloNumVar[] pVar = new IloNumVar[numOfBuses];
					for (int i = 0; i < numOfBuses; i++) {
						pVar[i] = cplex.numVar(2, 20, "p" + i);
						cplex.add(pVar[i]);
					}
					IloIntVar[] xVar = new IloIntVar[numOfBuses];
					for (int i = 0; i < numOfBuses; i++) {
						xVar[i] = cplex.intVar(0, 1, "x" + i);
						cplex.add(xVar[i]);
					}

					IloNumVar[] zLpVar = new IloNumVar[numOfLines];
					for (int i = 0; i < numOfLines; i++) {
						zLpVar[i] = cplex.numVar(0, 100, "zLp" + i);
						cplex.add(zLpVar[i]);
					}

					IloNumVar[] zLnVar = new IloNumVar[numOfLines];
					for (int i = 0; i < numOfLines; i++) {
						zLnVar[i] = cplex.numVar(0, 100, "zLn" + i);
						cplex.add(zLnVar[i]);
					}

					IloNumExpr P12 = cplex.numExpr();
					IloNumExpr P13 = cplex.numExpr();
					IloNumExpr P32 = cplex.numExpr();

					if (k % numOfBuses == 0) {
						P12 = cplex.sum(P12, cplex.prod(eVar[0], se[1] - sf[1]));
						P12 = cplex.sum(P12, cplex.prod(fVar[0], se[1] + sf[1]));
						P13 = cplex.sum(P13, cplex.prod(eVar[0], se[2] - sf[2]));
						P13 = cplex.sum(P13, cplex.prod(fVar[0], se[2] + sf[2]));
						P32 = cplex.sum(P32, se[2] * (se[1] - sf[1]));
						P32 = cplex.sum(P32, sf[2] * (se[1] + sf[1]));
						cplex.addEq(pVar[1], sp[1]);
						cplex.addEq(pVar[2], sp[2]);
					} else if (k % numOfBuses == 1) {
						P12 = cplex.sum(P12, cplex.prod(eVar[1], se[0] + sf[0]));
						P12 = cplex.sum(P12, cplex.prod(fVar[1], sf[0] - se[0]));
						P13 = cplex.sum(P13, se[0] * (se[2] - sf[2]));
						P13 = cplex.sum(P13, sf[0] * (se[2] + sf[2]));
						P32 = cplex.sum(P32, cplex.prod(eVar[1], se[2] + sf[2]));
						P32 = cplex.sum(P32, cplex.prod(fVar[1], sf[2] - se[2]));
						cplex.addEq(pVar[0], sp[0]);
						cplex.addEq(pVar[2], sp[2]);
					} else if (k % numOfBuses == 2) {
						P12 = cplex.sum(P12, se[0] * (se[1] - sf[1]));
						P12 = cplex.sum(P12, sf[0] * (se[1] + sf[1]));
						P13 = cplex.sum(P13, cplex.prod(eVar[2], se[0] + sf[0]));
						P13 = cplex.sum(P13, cplex.prod(fVar[2], sf[0] + se[0]));
						P32 = cplex.sum(P32, cplex.prod(eVar[2], se[1] - sf[1]));
						P32 = cplex.sum(P32, cplex.prod(fVar[2], se[1] + sf[1]));
						cplex.addEq(pVar[1], sp[1]);
						cplex.addEq(pVar[0], sp[0]);
					}

					cplex.addLe(P12, 200);
					cplex.addLe(-200, P12);
					cplex.addLe(P13, 200);
					cplex.addLe(-200, P13);
					cplex.addLe(P32, 200);
					cplex.addLe(-200, P32);

					for (int i = 0; i < numOfBuses; i++) {
						cplex.addLe(pVar[i], cplex.prod(20, xVar[i]));
						cplex.addLe(cplex.prod(2, xVar[i]), pVar[i]);
					}

					IloNumExpr obj = cplex.numExpr();
					cplex.addLe(obj, 10000);
					cplex.addLe(-10000, obj);

					obj = cplex.sum(obj, cplex.prod(pVar[0], 30));
					obj = cplex.sum(obj, cplex.prod(pVar[1], 2));
					obj = cplex.sum(obj, pVar[2]);

					obj = cplex.sum(obj, cplex.prod(multB[0],
							cplex.sum(P12, cplex.sum(pVar[0], cplex.sum(cplex.negative(P13), -10)))));
					obj = cplex.sum(obj, cplex.prod(multB[1],
							cplex.sum(P32, cplex.sum(pVar[1], cplex.sum(cplex.negative(P12), -5)))));
					obj = cplex.sum(obj, cplex.prod(multB[2],
							cplex.sum(P13, cplex.sum(pVar[2], cplex.sum(cplex.negative(P32), -5)))));

					obj = cplex.sum(obj, cplex.prod(multLp[0], cplex.sum(P12, cplex.sum(-1.5, zLpVar[0]))));
					obj = cplex.sum(obj, cplex.prod(multLp[1], cplex.sum(P13, cplex.sum(-1.5, zLpVar[1]))));
					obj = cplex.sum(obj, cplex.prod(multLp[2], cplex.sum(P32, cplex.sum(-1.5, zLpVar[2]))));

					obj = cplex.sum(obj,
							cplex.prod(multLn[0], cplex.sum(cplex.negative(P12), cplex.sum(-1.5, zLnVar[0]))));
					obj = cplex.sum(obj,
							cplex.prod(multLn[1], cplex.sum(cplex.negative(P13), cplex.sum(-1.5, zLnVar[1]))));
					obj = cplex.sum(obj,
							cplex.prod(multLn[2], cplex.sum(cplex.negative(P32), cplex.sum(-1.5, zLnVar[2]))));
					cplex.addMinimize(obj);
					////////////////////////////////

					if (cplex.solve()) {
						// cplex.exportModel("lpex1.lp");
						double q = cplex.getObjValue();

						double p12 = cplex.getValue(P12);
						double p13 = cplex.getValue(P13);
						double p32 = cplex.getValue(P32);

						double[] eVal = cplex.getValues(eVar);
						double[] fVal = cplex.getValues(fVar);
						double[] pVal = cplex.getValues(pVar);

						se[k % numOfBuses] = eVal[k % numOfBuses];
						sf[k % numOfBuses] = fVal[k % numOfBuses];
						sp[k % numOfBuses] = pVal[k % numOfBuses];

						szLp = cplex.getValues(zLpVar);
						szLn = cplex.getValues(zLnVar);

						slackB[0] = p12 + sp[0] - p13 - 10;
						slackB[1] = p32 + sp[1] - p12 - 5;
						slackB[2] = p13 + sp[2] - p32 - 5;

						slackLp[0] = p12 - 1.5 + szLp[0];
						slackLp[1] = p13 - 1.5 + szLp[1];
						slackLp[2] = p32 - 1.5 + szLp[2];
						slackLn[0] = -p12 - 1.5 + szLn[0];
						slackLn[1] = -p13 - 1.5 + szLn[1];
						slackLn[2] = -p32 - 1.5 + szLn[2];

						slackP[0] = sp[0] - 4 * (se[1] * se[0] + sf[0] * sf[0]);
						slackP[1] = sp[1] - 4 * (se[1] * se[1] + sf[1] * sf[1]);
						slackP[2] = sp[2] - 4 * (se[2] * se[2] + sf[2] * sf[2]);

						norm = 0;
						norm = slackB[0] * slackB[0] + slackB[1] * slackB[1] + slackB[2] * slackB[2]
								+ slackV[0] * slackV[0] + slackV[1] * slackV[1] + slackV[2] * slackV[2]
								+ slackLp[0] * slackLp[0] + slackLp[1] * slackLp[1] + slackLp[2] * slackLp[2]
								+ slackLn[0] * slackLn[0] + slackLn[1] * slackLn[1] + slackLn[2] * slackLn[2]
								+ slackP[0] * slackP[0] + slackP[1] * slackP[1] + slackP[2] * slackP[2];// +

						double rat = oldstep / step * Math.sqrt(oldnorm / norm);

						step = 0.996 * oldstep * Math.sqrt(oldnorm / norm);

						multB[0] = multB[0] + step * slackB[0];
						multB[1] = multB[1] + step * slackB[1];
						multB[2] = multB[2] + step * slackB[2];

						multLp[0] = multLp[0] + step * slackLp[0];
						multLp[1] = multLp[1] + step * slackLp[1];
						multLp[2] = multLp[2] + step * slackLp[2];

						multLn[0] = multLn[0] + step * slackLn[0];
						multLn[1] = multLn[1] + step * slackLn[1];
						multLn[2] = multLn[2] + step * slackLn[2];

						multP[0] = multP[0] + step * slackP[0];
						multP[1] = multP[1] + step * slackP[1];
						multP[2] = multP[2] + step * slackP[2];

						oldnorm = norm;
						oldstep = step;

						if (k / 20 == Math.round(k / 20))
							System.out.println(k % numOfBuses + "	" + penalty + "	" + q + "	" + Lagrangian + "	"
									+ step + "	" + norm + "	" + sp[0] + "	" + sp[1] + "	" + sp[2] + "	" + p12
									+ "	" + p13 + "	" + p32 + "	" + multB + "	" + multLp + "	" + multLn + "	" + se
									+ "	" + sf);

					}
					cplex.end();
				}
			} catch (IOException expt) {
				expt.printStackTrace();
				System.out.print(expt);
			}
		} catch (IloException e) {
			e.printStackTrace();
			// System.err.print("Concert exception '" + e + "' caught");
		}
	}
}
