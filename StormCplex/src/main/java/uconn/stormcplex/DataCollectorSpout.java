package uconn.stormcplex;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.Map;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class DataCollectorSpout implements IRichSpout {

	/**
	 * 
	 */
	private static final long serialVersionUID = 3969072189927047858L;

	SpoutOutputCollector _collector;

	int count = 0;

	public Integer nb_machines;
	public Integer nb_jobs;

	public Integer[][] cost;
	public Integer[][] time;
	public Integer[] constrain;

	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {

		_collector = collector;

		// System.out.println("Working Directory = " +
		// System.getProperty("user.dir"));
		// TODO Auto-generated method stub
		System.out.println("bbbbbbbbbbbbbbbbbbb" + conf.get("FILE_FULL_PATH"));
		try (BufferedReader br = Files.newBufferedReader(Paths.get((String) conf.get("FILE_FULL_PATH")))) {
			String[] problemSizeString = br.readLine().trim().split("\\s+");

			// number of machines
			nb_machines = Integer.parseInt(problemSizeString[0]);

			// number of jobs
			nb_jobs = Integer.parseInt(problemSizeString[1]);

			cost = new Integer[nb_machines][nb_jobs];
			time = new Integer[nb_machines][nb_jobs];
			constrain = new Integer[nb_machines];

			// set the number of machines
			for (int i = 0; i < nb_machines; i++) {
				String[] costTmp = br.readLine().trim().split("\\s+");
				for (int j = 0; j < nb_jobs; j++) {
					cost[i][j] = Integer.parseInt(costTmp[j]);
				}
			}

			// get the time spend for each job on each machine
			for (int i = 0; i < nb_machines; i++) {
				String[] timeTmp = br.readLine().trim().split("\\s+");
				for (int j = 0; j < nb_jobs; j++) {
					time[i][j] = Integer.parseInt(timeTmp[j]);
				}
			}

			// get the constrain for each job on each machine
			String[] constrainTmp = br.readLine().trim().split("\\s+");
			for (int i = 0; i < nb_machines; i++) {
				constrain[i] = Integer.parseInt(constrainTmp[i]);
			}

			// initialize lambda

			System.out.println("Number of machines:" + nb_machines);
			System.out.println("Number of jobs:" + nb_jobs);
			System.out.println();
		}

		catch (IOException e) {
			e.printStackTrace();
			System.out.print(e);
		}
	}

	public void close() {
		// TODO Auto-generated method stub

	}

	public void activate() {
		// TODO Auto-generated method stub

	}

	public void deactivate() {
		// TODO Auto-generated method stub

	}

	public void nextTuple() {
		// TODO Auto-generated method stub
		if (count == 0) {
			_collector.emit(new Values(nb_machines, nb_jobs, cost, time, constrain));
			count = 1;
		}

	}

	public void ack(Object msgId) {
		// TODO Auto-generated method stub

	}

	public void fail(Object msgId) {
		// TODO Auto-generated method stub

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("nb_machines", "nb_jobs", "cost", "time", "constrain"));
	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
