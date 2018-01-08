package uconn.stormcplex.salr.mp;

import uconn.stormcplex.MyConfig.ENVIRONMENT;

public class MyConfig {

	public static String DATA_COLLECTOR_SPOUT_NAME = "DataCollectorSpout";
	public static int DATA_COLLECTOR_SPOUT_AMOUNT = 1;

	public static String CORDINATOR_BOLT_NAME = "CordinatorBolt";
	public static int CORDINATOR_BOLT_AMOUNT = 1;

	public static String SUBPROBLEM_SOLVER_BOLT_NAME = "SubproblemSolverBolt";
	public static int SUBPROBLEM_SOLVER_BOLT_AMOUNT = 3;

	public static String LOCAL_TOPOLOGY_NAME = "localStormCplex";

	public static String FILE_NAME = "mp3bus";
	public static String FILE_PATH = "/home/uconncse/";
	// public static String FILE_FULL_PATH = "";

	public enum ENVIRONMENT {
		LOCAL, CLUSTER
	}

	public static ENVIRONMENT RUNNING_ENVIRONMENT;

}
