package uconn.stormcplex.update;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class DataCollectorSpout_update implements IRichSpout {
	SpoutOutputCollector _collector;
	int count = 0;
	public Integer numOfBuses;
	public Integer numOfLines;
	public Integer numOfScenarios;
	public double[][] multB, multLn, multLp;
	public double[][] slackB, slackLn, slackLp;
	public double[][] se, sf, sp, szLp, szLn;
	public double[] PVgen;

	@Override
	public void ack(Object arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void activate() {
		// TODO Auto-generated method stub

	}

	@Override
	public void close() {
		// TODO Auto-generated method stub

	}

	@Override
	public void deactivate() {
		// TODO Auto-generated method stub

	}

	@Override
	public void fail(Object arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void nextTuple() {
		// TODO Auto-generated method stub
		if (count == 0) {
			_collector.emit(new Values(numOfBuses, numOfLines, numOfScenarios, multB, multLn, multLp, slackB, slackLn,
					slackLp, se, sf, sp, szLp, szLn, PVgen));

			count = 1;
		}
	}

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		// TODO Auto-generated method stub
		_collector = collector;
		System.out.println("bbbbbbbbbbbbbbbbbbb" + conf.get("FILE_FULL_PATH"));
		try (BufferedReader br = Files.newBufferedReader(Paths.get((String) conf.get("FILE_FULL_PATH")))) {
			String[] problemSizeString = br.readLine().trim().split("\\s+");
			// number of machines
			numOfBuses = Integer.parseInt(problemSizeString[0]);
			System.out.println("Number of Buses:" + numOfBuses);

			// number of jobs
			numOfLines = Integer.parseInt(problemSizeString[1]);

			// number of scenarios
			numOfScenarios = Integer.parseInt(problemSizeString[2]);

			multB = new double[numOfBuses][numOfScenarios];
			multLn = new double[numOfLines][numOfScenarios];
			multLp = new double[numOfLines][numOfScenarios];
			slackB = new double[numOfBuses][numOfScenarios];
			slackLn = new double[numOfLines][numOfScenarios];
			slackLp = new double[numOfLines][numOfScenarios];
			for (int i = 0; i < numOfBuses; i++) {
				for (int j = 0; j < numOfScenarios; j++) {
					multB[i][j] = (double) 0;

					slackB[i][j] = (double) 0;

				}
			}

			for (int i = 0; i < numOfLines; i++) {
				for (int j = 0; j < numOfScenarios; j++) {
					slackLn[i][j] = (double) 0;
					slackLp[i][j] = (double) 0;
					multLn[i][j] = (double) 0;
					multLp[i][j] = (double) 0;
				}
			}

			se = new double[numOfBuses][numOfScenarios];
			sf = new double[numOfBuses][numOfScenarios];
			sp = new double[numOfBuses][numOfScenarios];
			szLp = new double[numOfLines][numOfScenarios];
			szLn = new double[numOfLines][numOfScenarios];
			for (int i = 0; i < numOfBuses; i++) {
				for (int j = 0; j < numOfScenarios; j++) {
					se[i][j] = 0;
					sf[i][j] = 0;
					sp[i][j] = 0;

				}
			}

			for (int i = 0; i < numOfLines; i++) {
				for (int j = 0; j < numOfScenarios; j++) {
					szLp[i][j] = 0;
					szLn[i][j] = 0;
				}
			}

			PVgen = new double[numOfScenarios];
			for (int i = 0; i < numOfScenarios; i++) {
				PVgen[i] = i + 1;
			}

		} catch (IOException e) {
			e.printStackTrace();
			System.out.print(e);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("numOfBuses", "numOfLines", "numOfScenarios", "multB", "multLn", "multLp", "slackB",
				"slackLn", "slackLp", "se", "sf", "sp", "szLp", "szLn", "PVgen"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
