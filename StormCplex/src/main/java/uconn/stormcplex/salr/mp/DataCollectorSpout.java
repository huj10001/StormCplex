package uconn.stormcplex.salr.mp;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
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
	public Integer numOfPVGen;
	public Integer numOfHours = 12;

	public Double tap_changer_cost;

	public PVGen[] pvGenArray;
	public Bus[][] busArray;
	// public Line[] lineArray;
	public double[][] pbbArray;
	public double[][] transArray;

	public Line[] lineArray;

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
			_collector.emit(new Values(numOfBuses, numOfLines, numOfScenarios, numOfPVGen, tap_changer_cost, pvGenArray,
					busArray, lineArray, pbbArray, transArray));
			count = 1;
		}
	}

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		// TODO Auto-generated method stub
		_collector = collector;
		try (BufferedReader br = Files.newBufferedReader(Paths.get((String) conf.get("FILE_FULL_PATH")))) {
			/* start line 1 */
			String[] problemSizeString = br.readLine().trim().split("\\s+");
			// number of Buses
			numOfBuses = Integer.parseInt(problemSizeString[0]);
			System.out.println("Number of Buses:" + numOfBuses);

			// number of machines
			numOfLines = Integer.parseInt(problemSizeString[1]);
			System.out.println("Number of Lines:" + numOfLines);

			// number of scenarios
			numOfScenarios = Integer.parseInt(problemSizeString[2]);
			System.out.println("Number of Scenarios:" + numOfScenarios);

			// number of pv gen
			numOfPVGen = Integer.parseInt(problemSizeString[3]);
			/* end line 1 */

			/* start line 2: tap changer cost */
			// tap changer cost
			problemSizeString = br.readLine().trim().split("\\s+");
			tap_changer_cost = Double.parseDouble(problemSizeString[0]);
			/* end line 2 */

			pvGenArray = new PVGen[numOfPVGen];
			for (int i = 0; i < numOfPVGen; i++) {
				/* start line 3: pvgen */
				problemSizeString = br.readLine().trim().split("\\s+");

				double[] tmppvGen = new double[numOfScenarios];
				for (int j = 0; j < numOfScenarios; j++) {
					tmppvGen[j] = Double.parseDouble(problemSizeString[j + 2]);
				}
				/* end line 3 */

				/* start line 4: pvvol */
				problemSizeString = br.readLine().trim().split("\\s+");
				double[] tmppvVol = new double[numOfScenarios];
				for (int j = 0; j < numOfScenarios; j++) {
					tmppvVol[j] = Double.parseDouble(problemSizeString[j + 2]);
				}
				/* end line 4 */
				pvGenArray[i] = new PVGen(problemSizeString[0], Integer.parseInt(problemSizeString[1]) - 1, tmppvGen,
						tmppvVol);
			}

			/* start line 5,6,7: bus load */
			busArray = new Bus[numOfBuses][numOfHours];
			for (int i = 0; i < numOfBuses; i++) {
				problemSizeString = br.readLine().trim().split("\\s+");
				for (int j = 0; j < numOfHours; j++) {
					busArray[i][j] = new Bus(problemSizeString[0], Integer.parseInt(problemSizeString[1]) - 1,
							Double.parseDouble(problemSizeString[j + 2]), Double.parseDouble(problemSizeString[14]));
					// System.out.println("hour"+j+"bus"+i+":"+problemSizeString[0]+"
					// "+(Integer.parseInt(problemSizeString[1]) - 1)+"
					// "+Double.parseDouble(problemSizeString[j+2])+"
					// "+Double.parseDouble(problemSizeString[14]));
				}
			}
			/* end line 5,6,7 */

			/* start line 8,9,10: g, b, constraint */
			lineArray = new Line[numOfLines];
			for (int i = 0; i < numOfLines; i++) {
				problemSizeString = br.readLine().trim().split("\\s+");
				lineArray[i] = new Line(Integer.parseInt(problemSizeString[0]) - 1,
						Integer.parseInt(problemSizeString[1]) - 1, Double.parseDouble(problemSizeString[2]),
						Double.parseDouble(problemSizeString[3]), Double.parseDouble(problemSizeString[4]));
			}
			/* end line 8,9,10 */

			/*
			 * start line 11,12,13: probability of being in a certain state (will affect
			 * cost)
			 */
			pbbArray = new double[numOfScenarios][numOfHours];
			for (int i = 0; i < numOfScenarios; i++) {
				problemSizeString = br.readLine().trim().split("\\s+");
				for (int j = 0; j < numOfHours; j++) {
					pbbArray[i][j] = Double.parseDouble(problemSizeString[j]);
				}
			}
			/* end line 11,12,13 */

			/*
			 * start line 14,15,16: transition probabilities among the three
			 * states/levels/scenarios
			 */
			transArray = new double[numOfScenarios][numOfScenarios];
			for (int i = 0; i < numOfScenarios; i++) {
				problemSizeString = br.readLine().trim().split("\\s+");
				for (int j = 0; j < numOfScenarios; j++) {
					transArray[i][j] = Double.parseDouble(problemSizeString[j]);
				}
			}
			/* end line 14,15,16 */

		} catch (IOException e) {
			e.printStackTrace();
			System.out.print(e);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("numOfBuses", "numOfLines", "numOfScenarios", "numOfPVGen", "tap_changer_cost",
				"pvGenArray", "busArray", "lineArray", "pbbArray", "transArray"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
