package uconn.stormcplex;

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
import ilog.cplex.IloCplex;

public class CordinatorBolt implements IBasicBolt {
	public Integer nb_machines;
	public Integer nb_jobs;

	public Integer[][] cost;
	public Integer[][] time;
	public Integer[] constrain;
	public Integer[] slack;
	Integer[][] unknown;
	Double[] lambda;

	// Integer id;

	double step_optimal;
	double c;
	double norm;
	double alfa;
	double M;
	double r;
	double p;
	double lastc;
	double lastnorm;
	double q;
	long myTime;

	int countFor300 = 0;
	int countFor5000 = 0;
	// long time

	int subProblemSolverID = 0;

	int count;

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("id", "unknown", "nb_machines", "nb_jobs", "cost", "time", "constrain", "lambda"));
	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	public void prepare(Map stormConf, TopologyContext context) {
		// TODO Auto-generated method stub
		c = 4.5 / 100;
		norm = 0;
		alfa = 0;
		M = 80;
		r = 0.2;
		p = 0;
		lastc = 4.5 / 100;
		lastnorm = 10;
		q = 0;
		count = 0;
	}

	public void execute(Tuple input, BasicOutputCollector collector) {
		// TODO Auto-generated method stub
		if (input.getSourceComponent().equals("DataCollectorSpout")) {
			myTime = System.currentTimeMillis();
			nb_machines = input.getInteger(0);
			nb_jobs = input.getInteger(1);
			cost = (Integer[][]) input.getValue(2);
			time = (Integer[][]) input.getValue(3);
			constrain = (Integer[]) input.getValue(4);
			slack = new Integer[nb_jobs];
			unknown = new Integer[nb_machines][nb_jobs];
			for (int i = 0; i < nb_machines; i++) {
				for (int j = 0; j < nb_jobs; j++) {
					unknown[i][j] = 0;
				}
			}
			lambda = new Double[nb_jobs];
			for (int i = 0; i < nb_jobs; i++) {
				lambda[i] = -100d;
			}

			IloCplex cplex;
			try {
				cplex = new IloCplex();
				cplex.setOut(null);

				IloIntVar[][] var = new IloIntVar[nb_machines][nb_jobs];
				IloIntExpr tmp = cplex.intExpr();
				for (int i = 0; i < nb_machines; i++) {
					for (int j = 0; j < nb_jobs; j++) {
						var[i][j] = cplex.intVar(0, 1, "x" + i + "_" + j);
						tmp = cplex.sum(tmp, cplex.prod(cost[i][j], var[i][j]));
						tmp = cplex.sum(tmp, cplex.prod(-100, var[i][j]));
					}
				}

				for (int i = 0; i < nb_jobs; i++) {
					tmp = cplex.diff(tmp, -100);
				}

				for (int i = 0; i < nb_machines; i++) {
					IloIntExpr tmpExpr = cplex.intExpr();
					for (int j = 0; j < nb_jobs; j++) {
						tmpExpr = cplex.sum(tmpExpr, cplex.prod(var[i][j], time[i][j]));
					}
					cplex.addLe(tmpExpr, constrain[i], "c " + i);
				}

				cplex.addMinimize(tmp);

				cplex.solve();
				for (int i = 0; i < nb_machines; i++) {
					double[] x = cplex.getValues(var[i]);
					for (int j = 0; j < nb_jobs; j++) {
						if (x[j] > 0.5d) {
							unknown[i][j] = 1;
						} else {
							unknown[i][j] = 0;
						}
					}
				}
				q = cplex.getObjValue();
				cplex.end();
			} catch (IloException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			for (int i = 0; i < nb_machines; i++) {
				collector.emit(new Values(i, unknown, nb_machines, nb_jobs, cost, time, constrain, lambda));
			}

		} else {

			count++;
			q = input.getDouble(0);
			Integer[][] tmpunknown = (Integer[][]) input.getValue(1);
			subProblemSolverID = input.getInteger(2);
			for (int i = 0; i < nb_jobs; i++) {
				unknown[subProblemSolverID][i] = tmpunknown[subProblemSolverID][i];
			}
		}
		for (int i = 0; i < nb_jobs; i++)
			slack[i] = -1;

		for (int i = 0; i < nb_machines; i++) {
			for (int j = 0; j < nb_jobs; j++) {
				slack[j] += unknown[i][j];
			}
		}

		norm = 0;
		for (int i = 0; i < nb_jobs; i++) {
			norm += (slack[i] * slack[i]);
		}
		norm = Math.sqrt(norm);
		/*
		 * if (count < 300) { M = 80; r = 0.2; } if (count > 300 && count < 600)
		 * { M = 80 / 1.5; r = 0.2 / 1.5; } if (count > 600 && count < 900) { M
		 * = 80 / 1.5 / 1.5; r = 0.2 / 1.5 / 1.5; } if (count > 900 && count <
		 * 1200) { M = 80 / 1.5 / 1.5 / 1.5; r = 0.2 / 1.5 / 1.5 / 1.5; } if
		 * (count > 1200 && count < 1500) { M = 80 / 1.5 / 1.5 / 1.5 / 1.5; r =
		 * 0.2 / 1.5 / 1.5 / 1.5 / 1.5; }
		 */
		countFor300++;
		countFor5000++;
		if (countFor300 > 300) {
			countFor300 = 0;
			M = M / 1.25;
			r = r / 1.25;
		}
		if (countFor5000 > 2000) {
			M = 80;
			r = 0.2;
			countFor5000 = 0;
		}
		p = 1 - 1 / Math.pow(count, r);
		alfa = 1 - 1 / M / Math.pow(count, p);
		c = alfa * lastc * lastnorm / (norm + 0.0001);
		for (int i = 0; i < nb_jobs; i++) {
			lambda[i] = lambda[i] + c * slack[i];
		}

		lastc = c;
		lastnorm = norm;
		long tmpTime = System.currentTimeMillis() - myTime;
		System.out.print("subProblemSolverID = " + subProblemSolverID + " " + "q = " + q + " " + "count = " + count
				+ " " + "time = " + tmpTime + " norm = " + norm + " step = " + c);
		// System.out.print("q = " + q + " \t");
		// System.out.print("count = " + count + " \t");

		System.out.println();
		if (count > 0) {
			collector.emit(new Values(subProblemSolverID, unknown, null, null, null, null, null, lambda));
		}

	}

	public void cleanup() {
		// TODO Auto-generated method stub

	}

}
