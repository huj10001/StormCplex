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
import ilog.concert.IloNumExpr;
import ilog.cplex.IloCplex;

public class SubproblemSolverBolt implements IBasicBolt {
	public Integer nb_machines;
	public Integer nb_jobs;

	public Integer[][] cost;
	public Integer[][] time;
	public Integer[] constrain;
	public Double[] slack;
	Integer[][] unknown;
	Double[] lambda;

	IloCplex cplex;

	private int iteration;

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("objvalue", "unknown", "id"));
	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	public void prepare(Map stormConf, TopologyContext context) {
		// TODO Auto-generated method stub
		iteration = 0;
	}

	public void execute(Tuple input, BasicOutputCollector collector) {
		// TODO Auto-generated method stub
		int k;
		if (iteration == 0) {
			k = input.getInteger(0);
			unknown = (Integer[][]) input.getValue(1);
			nb_machines = input.getInteger(2);
			this.nb_jobs = input.getInteger(3);
			cost = (Integer[][]) input.getValue(4);
			time = (Integer[][]) input.getValue(5);
			constrain = (Integer[]) input.getValue(6);
			lambda = (Double[]) input.getValue(7);
			iteration++;
		} else {
			k = input.getInteger(0);
			unknown = (Integer[][]) input.getValue(1);
			lambda = (Double[]) input.getValue(7);
		}

		if(nb_jobs == null) System.out.println("find it !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
		IloIntVar[] var = new IloIntVar[nb_jobs];

		try {
			cplex = new IloCplex();

			cplex.setOut(null);
			// this is to sum the goal
			IloNumExpr tmp = cplex.numExpr();
			for (int i = 0; i < nb_machines; i++) {
				for (int j = 0; j < nb_jobs; j++) {
					if (i == k) {
						var[j] = cplex.intVar(0, 1, "x" + i + "_" + j);
						tmp = cplex.sum(tmp, cplex.prod(cost[i][j], var[j]));
					} else {
						tmp = cplex.sum(tmp, cost[i][j] * unknown[i][j]);
					}
				}
			}

			for (int i = 0; i < nb_jobs; i++) {
				for (int j = 0; j < nb_machines; j++) {
					if (j == k) {
						tmp = cplex.sum(tmp, cplex.prod(var[i], lambda[i]));
					} else {
						tmp = cplex.sum(tmp, unknown[j][i] * lambda[i]);
					}
				}
				tmp = cplex.diff(tmp, lambda[i]);
			}
			cplex.addMinimize(tmp);
			cplex.setParam(IloCplex.IntParam.FracCuts, 2);
			cplex.setParam(IloCplex.LongParam.IntSolLim, 20);
			cplex.setParam(IloCplex.DoubleParam.EpGap, 0.00001);

			int i = k % nb_machines;
			IloIntExpr tmpExpr = cplex.intExpr();
			for (int j = 0; j < nb_jobs; j++) {
				tmpExpr = cplex.sum(tmpExpr, cplex.prod(var[j], time[i][j]));
			}
			cplex.addLe(tmpExpr, constrain[i], "c " + i);

			cplex.solve();

			double[] x = cplex.getValues(var);
			for (int j = 0; j < nb_jobs; j++) {
				if (x[j] > 0.5)
					unknown[k % nb_machines][j] = 1;
				else
					unknown[k % nb_machines][j] = 0;
			}
			collector.emit(new Values(cplex.getObjValue(), unknown, k));

			cplex.end();
			// System.out.println();
		} catch (IloException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void cleanup() {
		// TODO Auto-generated method stub

	}

}
