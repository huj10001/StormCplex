package uconn.stormcplex.salr;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class DataCollectorSpout implements IRichSpout {
	SpoutOutputCollector _collector;
	int count = 0;
	public Integer numOfBuses;
	public Integer numOfLines;
	public Integer numOfScenarios;
	public double[][] multB, multLn, multLp;
	public double[][] slackB, slackLn, slackLp;
	public double[][] se, sf, sp, szLp, szLn;
	public Integer[] sn, sb, sc;
	public double[] multT, slackT, PVgen;
	public Map<Line, Double> lineMap;
	public double[] busMult, busP;

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
			_collector.emit(new Values(numOfBuses, numOfLines, numOfScenarios, multT, multB, multLn, multLp, slackT,
					slackB, slackLn, slackLp, se, sf, sp, szLp, szLn, sn, sb, sc, PVgen, busMult, busP, lineMap));
			count = 1;
		}
	}

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		// TODO Auto-generated method stub
		_collector = collector;
		try (BufferedReader br = Files.newBufferedReader(Paths.get((String) conf.get("FILE_FULL_PATH")))) {
			String[] problemSizeString = br.readLine().trim().split("\\s+");
			// number of Buses
			numOfBuses = Integer.parseInt(problemSizeString[0]);
			System.out.println("Number of Buses:" + numOfBuses);

			// number of machines
			numOfLines = Integer.parseInt(problemSizeString[1]);
			System.out.println("Number of Buses:" + numOfBuses);

			// number of scenarios
			numOfScenarios = Integer.parseInt(problemSizeString[2]);

			multT = new double[numOfScenarios];
			multB = new double[numOfBuses][numOfScenarios];
			multLn = new double[numOfLines][numOfScenarios];
			multLp = new double[numOfLines][numOfScenarios];

			slackT = new double[numOfScenarios];
			slackB = new double[numOfBuses][numOfScenarios];
			slackLn = new double[numOfLines][numOfScenarios];
			slackLp = new double[numOfLines][numOfScenarios];

			for (int i = 0; i < numOfBuses; i++) {
				for (int j = 0; j < numOfScenarios; j++) {
					multB[i][j] = 0d;
					slackB[i][j] = 0d;
				}
			}

			for (int i = 0; i < numOfLines; i++) {
				for (int j = 0; j < numOfScenarios; j++) {
					slackLn[i][j] = 0d;
					slackLp[i][j] = 0d;
					multLn[i][j] = 0d;
					multLp[i][j] = 0d;
				}
			}

			se = new double[numOfBuses][numOfScenarios];
			sf = new double[numOfBuses][numOfScenarios];
			sp = new double[numOfBuses][numOfScenarios];
			szLp = new double[numOfLines][numOfScenarios];
			szLn = new double[numOfLines][numOfScenarios];
			sn = new Integer[numOfScenarios];
			sb = new Integer[numOfScenarios];
			sc = new Integer[numOfScenarios];

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

			for (int i = 0; i < numOfScenarios; i++) {
				sn[i] = 1;
				sb[i] = 1;
				sc[i] = 1;
			}

			problemSizeString = br.readLine().trim().split("\\s+");
			PVgen = new double[numOfScenarios];
			for (int i = 0; i < numOfScenarios; i++) {
				PVgen[i] = Double.parseDouble(problemSizeString[i]);
				multT[i] = 0d;
				slackT[i] = 0d;
			}

			busMult = new double[numOfBuses];
			busP = new double[numOfBuses];
			for (int i = 0; i < numOfBuses; i++) {
				problemSizeString = br.readLine().trim().split("\\s+");
				busP[i] = Double.parseDouble(problemSizeString[0]);
				busMult[i] = Double.parseDouble(problemSizeString[1]);
			}

			lineMap = new HashMap<>();
			for (int i = 0; i < numOfLines; i++) {
				problemSizeString = br.readLine().trim().split("\\s+");
				Line line = new Line(Integer.parseInt(problemSizeString[0]) - 1,
						Integer.parseInt(problemSizeString[1]) - 1);
				lineMap.put(line, Double.parseDouble(problemSizeString[2]));
			}

		} catch (IOException e) {
			e.printStackTrace();
			System.out.print(e);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("numOfBuses", "numOfLines", "numOfScenarios", "multT", "multB", "multLn", "multLp",
				"slackT", "slackB", "slackLn", "slackLp", "se", "sf", "sp", "szLp", "szLn", "sn", "sb",
				"sc", "PVgen", "busMult", "busP", "lineMap"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
