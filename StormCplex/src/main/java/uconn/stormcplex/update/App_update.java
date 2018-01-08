package uconn.stormcplex.update;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;

/**
 * Hello world!
 *
 */
public class App_update {
	public static TopologyBuilder buildTopology(String SubproblemBoltAmount) {

		// typical topology builder
		TopologyBuilder builder = new TopologyBuilder();

		// Spout reads data from files and send data
		builder.setSpout(MyConfig_update.DATA_COLLECTOR_SPOUT_NAME, new DataCollectorSpout_update());

		// Bolt get data and send data
		builder.setBolt(MyConfig_update.CORDINATOR_BOLT_NAME, new CordinatorBolt_update(), MyConfig_update.CORDINATOR_BOLT_AMOUNT)
				.shuffleGrouping(MyConfig_update.DATA_COLLECTOR_SPOUT_NAME)
				.shuffleGrouping(MyConfig_update.SUBPROBLEM_SOLVER_BOLT_NAME);
//
//		// window bolts receive data and cache them in window
		builder.setBolt(MyConfig_update.SUBPROBLEM_SOLVER_BOLT_NAME, new SubproblemSolverBolt_update(),
				Integer.parseInt(SubproblemBoltAmount)).shuffleGrouping(MyConfig_update.CORDINATOR_BOLT_NAME);

		return builder;
	}

	public static Config buildConfig(int nb_workers, String dataFileName) {
		Config conf = new Config();
		// conf.put(Config.TOPOLOGY_DEBUG, false);
		conf.put(Config.TOPOLOGY_WORKERS, nb_workers);

		if (MyConfig_update.RUNNING_ENVIRONMENT == MyConfig_update.ENVIRONMENT.CLUSTER) {
			conf.put("FILE_FULL_PATH", MyConfig_update.FILE_PATH + dataFileName);
		} else {
			conf.put("FILE_FULL_PATH", dataFileName);
		}

		conf.put(Config.TOPOLOGY_WORKER_CHILDOPTS,
				"-Djava.library.path='/opt/ibm/ILOG/CPLEX_Studio127/cplex/bin/x86-64_linux/'");
		conf.setDebug(false);

		// Log all storm metrics
		// conf.registerMetricsConsumer(org.apache.storm.metric.LoggingMetricsConsumer.class);

		// Add in per worker CPU measurement
		// Map<String, String> workerMetrics = new HashMap<String, String>();
		// workerMetrics.put("CPU", "org.apache.storm.metrics.sigar.CPUMetric");
		// conf.put(Config.TOPOLOGY_WORKER_METRICS, workerMetrics);
		conf.put(Config.TOPOLOGY_ACKER_EXECUTORS, 0);
		// conf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 2000 *
		// MyConfig.WINDOW_LENGTH_IN_SEC + MyConfig.ACK_OFFSET);
		// conf.put(Config.SUPERVISOR_WORKER_TIMEOUT_SECS, 2000 *
		// MyConfig.WINDOW_LENGTH_IN_SEC + MyConfig.ACK_OFFSET);
		return conf;
	}

	public static Config buildConfig() {
		return buildConfig(15, MyConfig_update.FILE_NAME);
	}

	public static void main(String[] args) {

		if (args != null && args.length > 0) {
			// If there are arguments, we must be on a cluster
			try {
				// MyConfig.DEPLOYMENT = Deployment.REMOTE;
				MyConfig_update.RUNNING_ENVIRONMENT = MyConfig_update.ENVIRONMENT.CLUSTER;
				Config conf = buildConfig(Integer.parseInt(args[2]), args[3]);
				TopologyBuilder builder = buildTopology(args[1]);
				StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
			} catch (AlreadyAliveException e) {
				e.printStackTrace();
			} catch (InvalidTopologyException e) {
				e.printStackTrace();
			} catch (AuthorizationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} else {
			// Otherwise, we are running locally
			// conf.setNumWorkers(3);
			MyConfig_update.RUNNING_ENVIRONMENT = MyConfig_update.ENVIRONMENT.LOCAL;
			Config conf = buildConfig();
			// MyConfig.DEPLOYMENT = Deployment.LOCAL;
			TopologyBuilder builder = buildTopology("" + MyConfig_update.SUBPROBLEM_SOLVER_BOLT_AMOUNT);
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology(MyConfig_update.LOCAL_TOPOLOGY_NAME, conf, builder.createTopology());
		}
	}
}