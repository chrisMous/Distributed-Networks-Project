/*
   This class contains messages that are passed around the Network that
   enable operations to communicate with the Controller,Client and DStores
*/
public class NetworkProtocol {

	//used for the store operation
	public final static String STORE = "STORE";
	public final static String STORE_TO = "STORE_TO";
	public final static String ACK = "ACK";
	public final static String STORE_ACK = "STORE_ACK";
	public final static String STORE_COMPLETE = "STORE_COMPLETE";

	//used for the load operation
	public final static String LOAD = "LOAD";
	public final static String LOAD_FROM = "LOAD_FROM";
	public final static String LOAD_DATA = "LOAD_DATA";
	
	//used for the remove operation
	public final static String REMOVE = "REMOVE";
	public final static String REMOVE_ACK = "REMOVE_ACK";
	public final static String REMOVE_COMPLETE = "REMOVE_COMPLETE";

	//used for list and rebalance operations
	public final static String LIST = "LIST";
	public final static String REBALANCE = "REBALANCE";
	public final static String REBALANCE_COMPLETE = "REBALANCE_COMPLETE";
        public final static String REBALANCE_STORE = "REBALANCE_STORE";
	//used for failure handling (errors)
	public final static String NOT_ENOUGH_DSTORES = "ERROR_NOT_ENOUGH_DSTORES";
	public final static String FILE_ALREADY_EXISTS = "ERROR_FILE_ALREADY_EXISTS";
	public final static String FILE_DOES_NOT_EXIST = "ERROR_FILE_DOES_NOT_EXIST";
	public final static String ERROR_LOAD = "ERROR_LOAD";
	
	public final static String JOIN = "JOIN";

}
