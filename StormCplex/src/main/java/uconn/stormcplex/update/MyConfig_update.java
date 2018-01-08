package uconn.stormcplex.update;

import uconn.stormcplex.MyConfig.ENVIRONMENT;

public class MyConfig_update {

	public static String DATA_COLLECTOR_SPOUT_NAME = "DataCollectorSpout_update";
	public static int DATA_COLLECTOR_SPOUT_AMOUNT = 1;

	public static String CORDINATOR_BOLT_NAME = "CordinatorBolt_update";
	public static int CORDINATOR_BOLT_AMOUNT = 1;

	public static String SUBPROBLEM_SOLVER_BOLT_NAME = "SubproblemSolverBolt_update";
	public static int SUBPROBLEM_SOLVER_BOLT_AMOUNT = 3;

	public static String LOCAL_TOPOLOGY_NAME = "localStormCplex";

	public static String FILE_NAME = "dataThreeBuses";
	public static String FILE_PATH = "/home/uconncse/";
	// public static String FILE_FULL_PATH = "";

	public enum ENVIRONMENT {
		LOCAL, CLUSTER
	}

	public static ENVIRONMENT RUNNING_ENVIRONMENT;

}
