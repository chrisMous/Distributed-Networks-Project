
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class Controller {
    private final Logger logger;
    private final int cport;
    private final int r;
    private final int timeout;
    private final int rebalancePeriod;
    private String[] inputs;
    private final ConcurrentHashMap<Integer, ArrayList<String>> portDictionary = new ConcurrentHashMap<>();
    private final Object joinLock = new Object();
    private volatile Boolean isRebalancing = false;
    private volatile Long rebalanceDuration;
    private final AtomicInteger noOfDstores = new AtomicInteger(0);
    private Boolean rebalanceList = false;
    private final ConcurrentHashMap<String, ArrayList<Integer>> fileDictionary = new ConcurrentHashMap<>();
    private final List<Integer> activePorts = Collections.synchronizedList(new ArrayList<>());
    private final ConcurrentHashMap<Integer, Socket> portMap = new ConcurrentHashMap<>();
    private final ArrayList<String> fileList = new ArrayList<>();
    private final ConcurrentHashMap<String, ArrayList<Integer>> filesToLoad = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Integer> fileSizes = new ConcurrentHashMap<>();
    private final List<String> beingRemoved = Collections.synchronizedList(new ArrayList<>());
    private final List<String> beingStored = Collections.synchronizedList(new ArrayList<>());
    private final ConcurrentHashMap<String, ArrayList<Integer>> toStoreMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, ArrayList<Integer>> toRemoveMap = new ConcurrentHashMap<>();
    private final Object storeLock = new Object();
    private final Object removeLock = new Object();
    private final AtomicInteger rebalanced = new AtomicInteger(0);
    private final ConcurrentHashMap<String, Integer> forRebalance = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Integer> forRebalanceRemove = new ConcurrentHashMap<>();
    private int noOfRebalances = 0;
    private volatile Boolean isListing = false;
    private final Object defaultLock = new Object();
    boolean isDStore = false;
    private Integer port = 0;
    private final ServerSocket serverSocket;
    public Controller(int cport, int r, int timeout, int rebalancePeriod) throws IOException {
        this.cport = cport;
        this.r = r;
        this.timeout = timeout;
        this.rebalancePeriod = rebalancePeriod;
        this.logger = new Logger();
        serverSocket = new ServerSocket(cport);

        this.initialise();
    }

    public static void main(String[] args) throws IOException {
        Controller controller = new Controller(Integer.parseInt(args[0]), Integer.parseInt(args[1]), Integer.parseInt(args[2]),Integer.parseInt(args[3]) );
    }

    public void initialise() {
        try {

            initialiseRebalance(serverSocket);
            while (true) {
                Socket controllerSocket = serverSocket.accept();
                initialiseControllerThread(controllerSocket);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void initialiseControllerThread(Socket controllerSocket) {
        new Thread(() -> {

            try {
                PrintWriter transmitter = new PrintWriter(controllerSocket.getOutputStream(), true);
                BufferedReader receiver = new BufferedReader(new InputStreamReader(controllerSocket.getInputStream()));
                String input;

                while (true) {

                    input = receiver.readLine();
                    logger.logMessageReceived(controllerSocket, input);
                    if (input != null) {
                        String output;
                        inputs = input.split(" ");

                        if (inputs.length != 1) {
                            output = inputs[0];

                        } else {
                            output = input.trim();
                            inputs[0] = output;
                        }

                        handleMessages(output, transmitter, receiver, controllerSocket);
                    }
                    else {
                        logger.logError("RECEIVED EMPTY MESSAGE");
                    }
                }

            } catch (IOException e) {
                e.printStackTrace();
                if(isDStore) {
                    logger.logError("DSTORE CRASH!");
                    noOfDstores.decrementAndGet();
                    waitRebalance();
                    synchronized (defaultLock){
                        handleClosedPorts(port);
                    }

                }
            }
        }).start();
    }


    public void handleMessages(String message, PrintWriter transmitter, BufferedReader receiver, Socket controllerSocket) throws IOException {

        if (message.equals(NetworkProtocol.JOIN)) {
            handleJoin(controllerSocket);
        } else if (message.equals(NetworkProtocol.LIST)) {
            handleList(transmitter, controllerSocket);
        } else if (message.equals(NetworkProtocol.LOAD) || message.equals("RELOAD")) {
            handleLoad(transmitter, message, controllerSocket);
        } else if (message.equals(NetworkProtocol.STORE)) {
            handleStore(transmitter, controllerSocket);
        } else if (message.equals(NetworkProtocol.STORE_ACK)) {
            handleStoreAck(controllerSocket);
        } else if (message.equals(NetworkProtocol.REMOVE)) {
            handleRemove(transmitter,controllerSocket);
        } else if (message.equals(NetworkProtocol.FILE_DOES_NOT_EXIST) || message.equals(NetworkProtocol.REMOVE_ACK)) {
            handleRemoveAck(controllerSocket);
        } else if (isRebalancing && message.equals(NetworkProtocol.REBALANCE_COMPLETE)) {
            rebalanced.incrementAndGet();
        } else logger.logError("Incorrect Message Received");

    }

    public void handleJoin(Socket controllerSocket) throws IOException {
        if (inputs.length == 2) {
            port = Integer.parseInt(inputs[1]);

            if (!portDictionary.containsKey(port)) {

                synchronized (joinLock) {
		   if(noOfDstores.get() == r) {
                    waitRebalance();
		}
                    portDictionary.put(port, new ArrayList<>());

                    portMap.put(port,controllerSocket);
                    noOfDstores.incrementAndGet();

                    logger.logDStore(controllerSocket, port);
		    if(noOfDstores.get() > r){
                    isDStore = true;
                    this.isRebalancing = true;
                    this.rebalanceDuration = System.currentTimeMillis();
			}
                }
            } else {
                logger.logError("Cannot Add DStore It Already Exists");
                controllerSocket.close();
            }

        } else logger.logError("Incorrect JOIN message format");
    }

    public void handleList(PrintWriter transmitter, Socket controllerSocket) {
        logger.log("Listing Files");
        if (rebalanceList && isDStore) {
            ArrayList<String> files = new ArrayList<>(Arrays.asList(inputs));
            files.remove(0);
            portDictionary.put(port, files);
            portMap.put(port,controllerSocket);
            for (String file : files) {
                if (!fileDictionary.get(file).contains(port))
                    fileDictionary.get(file).add(port);
                fileDictionary.computeIfAbsent(file, k -> new ArrayList<Integer>());
            }
            activePorts.add(port);

        } else if (!isDStore) {
            if (inputs.length == 1) {
                if (r > noOfDstores.get()) {
                    logger.logMessageSent(controllerSocket, NetworkProtocol.NOT_ENOUGH_DSTORES);
                    transmitter.println(NetworkProtocol.NOT_ENOUGH_DSTORES);
                }
                else if (fileList.size() != 0) {
                    String listToTransmit = " " + String.join(" ", fileList);
                    logger.logMessageSent(controllerSocket, listToTransmit);
                    transmitter.println("LIST" + listToTransmit);
                }
                else {
                    logger.logMessageSent(controllerSocket, "LIST");
                    transmitter.println(NetworkProtocol.LIST);
                }
            } else logger.logError("Incorrect LIST message format");
        }
    }

    public void handleLoad(PrintWriter transmitter, String message, Socket controllerSocket) {
        if (inputs.length == 2) {
            if (r > noOfDstores.get()) {
                logger.logError(NetworkProtocol.NOT_ENOUGH_DSTORES);
                transmitter.println(NetworkProtocol.NOT_ENOUGH_DSTORES);
            } else if (!fileList.contains(inputs[1])) {
                logger.logError(NetworkProtocol.FILE_DOES_NOT_EXIST);
                transmitter.println(NetworkProtocol.FILE_DOES_NOT_EXIST);
            } else {
                checkConcurrentLoad(transmitter, message);

            waitRebalance();

            if (message.equals(NetworkProtocol.LOAD)) {
                filesToLoad.put(inputs[1], new ArrayList<>(fileDictionary.get(inputs[1])));
                logger.logMessageSent(controllerSocket, NetworkProtocol.LOAD_FROM + " " + filesToLoad.get(inputs[1]).get(0) + " " + fileSizes.get(inputs[1]));
                transmitter.println(NetworkProtocol.LOAD_FROM + " " + filesToLoad.get(inputs[1]).get(0) + " " + fileSizes.get(inputs[1]));
                filesToLoad.get(inputs[1]).remove(0);
            } else if (!(filesToLoad.get(inputs[1]) == null) && !filesToLoad.get(inputs[1]).isEmpty()) {
                logger.logMessageSent(controllerSocket, NetworkProtocol.LOAD_FROM + " " + filesToLoad.get(inputs[1]).get(0) + " " + fileSizes.get(inputs[1]));
                transmitter.println(NetworkProtocol.LOAD_FROM + " " + filesToLoad.get(inputs[1]).get(0) + " " + fileSizes.get(inputs[1]));
                filesToLoad.get(inputs[1]).remove(0);
            }
            else {
                logger.logError(NetworkProtocol.ERROR_LOAD);
                transmitter.println(NetworkProtocol.ERROR_LOAD);
            }

        }
        } else {
            logger.logError("Incorrect LOAD message format");
            transmitter.println(NetworkProtocol.ERROR_LOAD);
        }
    }

    public void handleStore(PrintWriter transmitter, Socket controllerSocket) {
        logger.log("Storing File");
        if (inputs.length == 3) {
            if (r > noOfDstores.get()) {
                logger.logError(NetworkProtocol.NOT_ENOUGH_DSTORES);
                transmitter.println(NetworkProtocol.NOT_ENOUGH_DSTORES);
            } else if (fileList.contains(inputs[1]) || beingRemoved.contains(inputs[1])) {
                logger.logError(NetworkProtocol.FILE_ALREADY_EXISTS);
                transmitter.println(NetworkProtocol.FILE_ALREADY_EXISTS);
            } else {
                synchronized (defaultLock) {
                    checkConcurrentStore(transmitter);
                }


                String filename = inputs[1];
                Integer size = Integer.valueOf(inputs[2]);
                String toStore = String.join(" ", toStore(r));
                toStoreMap.put(filename, new ArrayList<>());

                logger.logMessageSent(controllerSocket, NetworkProtocol.STORE_TO + " " + toStore);
                transmitter.println(NetworkProtocol.STORE_TO + " " + toStore);

                long timeoutDuration = System.currentTimeMillis() + this.timeout;

                while (System.currentTimeMillis() <= timeoutDuration) {
                    if (toStoreMap.get(filename).size() >= r) {

                        logger.log(NetworkProtocol.STORE_COMPLETE);
                        transmitter.println(NetworkProtocol.STORE_COMPLETE);

                        fileList.add(filename);
                        fileDictionary.put(filename, toStoreMap.get(filename));
                        fileSizes.put(filename, size);

                        break;
                    }
                }

                synchronized (storeLock) {
                    toStoreMap.remove(filename);
                }
                beingStored.remove(filename);
            }
        } else logger.logError("Incorrect STORE message format");
    }

    public void handleStoreAck(Socket controllerSocket) {
        String filename = inputs[1];

        synchronized (storeLock) {
            if (toStoreMap.containsKey(filename))
                toStoreMap.get(filename).add(port);
        }
        portDictionary.get(port).add(filename);
    }

    public void handleRemove(PrintWriter transmitter, Socket controllerSocket) throws IOException {
        if (inputs.length == 2) {
            if (r > noOfDstores.get()) {
                logger.logError(NetworkProtocol.NOT_ENOUGH_DSTORES);
                transmitter.println(NetworkProtocol.NOT_ENOUGH_DSTORES);
            } else if (!fileList.contains(inputs[1]) || beingStored.contains(inputs[1])) {
                logger.logError(NetworkProtocol.FILE_DOES_NOT_EXIST);
                transmitter.println(NetworkProtocol.FILE_DOES_NOT_EXIST);
            } else {
                synchronized (defaultLock) {
                    checkConcurrentRemove(transmitter);
                }
                String filename = inputs[1];
                toRemoveMap.put(filename, new ArrayList<>(fileDictionary.get(filename)));
                forRebalance.remove(filename);

                synchronized (removeLock) {
                    for (int port : toRemoveMap.get(filename)) {
                        Socket newSocket = portMap.get(port);

                        PrintWriter newWriter = new PrintWriter(newSocket.getOutputStream(), true);
                        logger.logMessageSent(newSocket, NetworkProtocol.REMOVE + " " + filename);
                        newWriter.println(NetworkProtocol.REMOVE + " " + filename);
                    }
                }

                long timeoutDuration = System.currentTimeMillis() + this.timeout;
                while (timeoutDuration >= System.currentTimeMillis()) {
                    if (toRemoveMap.get(filename).isEmpty()) {
                        fileDictionary.remove(filename);
                        fileList.remove(filename);
                        logger.logMessageSent(controllerSocket, NetworkProtocol.REMOVE_COMPLETE);
                        transmitter.println(NetworkProtocol.REMOVE_COMPLETE);
                        break;
                    }
                }
                beingRemoved.remove(filename);
                toRemoveMap.remove(filename);
            }
        } else logger.logError("Incorrect REMOVE message format");
    }

    public void handleRemoveAck(Socket controllerSocket) {
        String filename = inputs[1];
        synchronized (removeLock) {
            if (toRemoveMap.containsKey(filename))
                toRemoveMap.get(filename).remove(port);
        }
        fileDictionary.get(filename).remove(port);
        portDictionary.get(port).remove(filename);

    }

    public void checkConcurrentStore(PrintWriter transmitter) {
        if (beingStored.contains(inputs[1])) {
            logger.logError(NetworkProtocol.FILE_ALREADY_EXISTS);
            transmitter.println(NetworkProtocol.FILE_ALREADY_EXISTS);
        } else {
            if(fileList.size() > 0) {
                waitRebalance();
            }
            logger.log("Storing file Index");
            beingStored.add(inputs[1]);
        }
    }

    public void checkConcurrentRemove(PrintWriter transmitter) {
        if (fileList.contains(inputs[1]) || beingRemoved.contains(inputs[1])) {
            logger.logError(NetworkProtocol.FILE_DOES_NOT_EXIST);
            transmitter.println(NetworkProtocol.FILE_DOES_NOT_EXIST);
        } else {

            if(fileList.size() > 0) {
                waitRebalance();
            }
            logger.log("Removing file Index");
            fileSizes.remove(inputs[1]);
            beingRemoved.add(inputs[1]);
        }
    }

    public void checkConcurrentLoad(PrintWriter transmitter, String message) {
        if (beingStored.contains(inputs[1]) || beingRemoved.contains(inputs[1])) {
            if (!message.equals(NetworkProtocol.LOAD)) {
                logger.logError(NetworkProtocol.ERROR_LOAD);
                transmitter.println(NetworkProtocol.ERROR_LOAD);
            } else {
                logger.logError(NetworkProtocol.FILE_DOES_NOT_EXIST);
                transmitter.println(NetworkProtocol.FILE_DOES_NOT_EXIST);
            }
        }
    }

    private void initialiseRebalance(ServerSocket serverSocket) {
        new Thread(() -> {

            if (noOfRebalances == 0) {
                try {
                    Thread.sleep(this.rebalancePeriod * 1000L);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }

            while (true) {
                rebalanceDuration = System.currentTimeMillis() + this.rebalancePeriod;

                while (rebalanceDuration >= System.currentTimeMillis()) {
                    continue;
                }

                handleRebalance(serverSocket);
                isRebalancing = false;
            }
        }).start();
    }


    public void handleRebalance(ServerSocket serverSocket) {
        try {
            if (r > noOfDstores.get()) {
                logger.logError(NetworkProtocol.NOT_ENOUGH_DSTORES);
                this.isRebalancing = false;
                return;
            }

            synchronized (defaultLock) {
                logger.log("Waiting for Store or Remove operations to rebalance");
                while ((!beingStored.isEmpty() || !beingRemoved.isEmpty())) continue;
                isRebalancing = true;
            }

            logger.log("Starting Rebalance Operation");
            forRebalanceRemove.clear();
            int dStores = portMap.size();
            ArrayList<Integer> portFailures = new ArrayList<>();

            listDstores(dStores, portFailures);
            rebalanceAll();

            int rSize = portMap.size();
            long timeoutDuration = System.currentTimeMillis() + this.timeout;
            while (timeoutDuration >= System.currentTimeMillis()) {
                if (rebalanced.get() >= rSize) {
                    logger.log("Rebalance Operation Complete");
                    break;
                }
            }

            noOfRebalances++;
            isRebalancing = false;
            rebalanced.set(0);


        } catch (Exception e) {
            logger.logError("Rebalance Failed");
            rebalanced.set(0);
            isRebalancing = false;
        }
    }

    private String[] toStore(int r) {
        Integer[] ports = new Integer[r];

        for (Integer port : getFileCount(portDictionary).keySet()) {
            int max = 0;

            for (int i = 0; i < r; i++) {

                if (ports[i] != null && getFileCount(portDictionary).get(ports[i]) > getFileCount(portDictionary).get(ports[max])) {
                    max = i;
                }

                if (ports[i] == null) {
                    max = i;
                    ports[i] = port;
                    break;
                }

            }
            if (getFileCount(portDictionary).get(port) < getFileCount(portDictionary).get(ports[max])) {
                ports[max] = port;
            }
        }

        String[] returnPorts = new String[r];

        for (int j = 0; j < r; j++) {
            returnPorts[j] = ports[j].toString();
        }
        return returnPorts;
    }


    private synchronized Integer[] toStoreFile(int r, String file) {
        Integer[] ports = new Integer[r];

        for (Integer port : getFileCount(portDictionary).keySet()) {
            int max = 0;
            for (int i = 0; i < r; i++) {
                if (ports[i] == null) {
                    max = i;
                    ports[i] = port;
                    break;
                }
                if (ports[i] != null && getFileCount(portDictionary).get(ports[i]) > getFileCount(portDictionary).get(ports[max]) && !portDictionary.get(ports[i]).contains(file)) {
                    max = i;
                }
            }
            if (getFileCount(portDictionary).get(port) <= getFileCount(portDictionary).get(ports[max]) && !portDictionary.get(port).contains(file)) {
                ports[max] = port;

            } else if (getFileCount(portDictionary).get(port) > getFileCount(portDictionary).get(ports[max]) && portDictionary.get(ports[max]).contains(file)) {
                ports[max] = port;
            }
        }

        return ports;
    }

    private ConcurrentHashMap<Integer, Integer> getFileCount(ConcurrentHashMap<Integer, ArrayList<String>> portDictionary) {

        ConcurrentHashMap<Integer, Integer> map = new ConcurrentHashMap<>();
        for (Integer key : portDictionary.keySet()) {
            map.put(key, portDictionary.get(key).size());
        }
        return map;
    }

    private synchronized void handleClosedPorts(Integer p) {
        for (String file : portDictionary.get(p)) {
            if (forRebalance.isEmpty()) {
                forRebalance.put(file, 1);
            } else forRebalance.put(file, forRebalance.get(file) + 1);
        }

        portDictionary.remove(p);
        portMap.remove(p);

        for (String file : fileDictionary.keySet()) {
            if (fileDictionary.get(file).contains(p)) {
                fileDictionary.get(file).remove(p);
            } else if (!fileSizes.containsKey(file)) {
                fileDictionary.remove(file);
            }
        }
    }

    public void listDstores(int dStores, ArrayList<Integer> portFailures) {
        logger.log("Listing All Dstores: ");
        rebalanceList = true;

        for (int port : portMap.keySet()) {
            try {
                PrintWriter out = new PrintWriter(portMap.get(port).getOutputStream(), true);
                logger.logMessageSent(portMap.get(port), NetworkProtocol.LIST);
                out.println(NetworkProtocol.LIST);

            } catch (Exception e) {
                portFailures.add(port);
                dStores--;
                logger.logError("Dstore : " + port + " Failed");
            }
        }
        activePorts.clear();
        long timeoutDuration = System.currentTimeMillis() + this.timeout;
        while (timeoutDuration >= System.currentTimeMillis()) {
            if (activePorts.size() >= dStores) {
                logger.log("LIST Received from all dStores");
                break;
            }
        }
        rebalanceList = false;

        for (int port : portFailures)
            handleClosedPorts(port);

        for (String file : fileDictionary.keySet()) {
            if (r < fileDictionary.get(file).size())
                forRebalanceRemove.put(file, fileDictionary.get(file).size() - r);
        }

        logger.log("LIST Complete");

    }

    public void rebalanceAll() {
        ConcurrentHashMap<Integer, ArrayList<String>> temporaryPorts = new ConcurrentHashMap<>();

        for (int port : portMap.keySet()) {
            ArrayList<String> temporaryFiles = new ArrayList<>();
            int sendCount = 0;
            int removeCount = 0;
            StringBuilder sendString = new StringBuilder();
            StringBuilder removeString = new StringBuilder();

            for (String file : portDictionary.get(port)) {

                if (forRebalance.containsKey(file)) {
                    Integer[] toSend = toStoreFile(forRebalance.get(file), file);

                    for (int prt : toSend) {
                        temporaryFiles.add(file);
                        fileDictionary.get(file).add(prt);
                    }

                    String[] toSendStringArr = new String[toSend.length];

                    for (int i = 0; i < toSend.length; i++) {
                        toSendStringArr[i] = toSend[i].toString();
                    }

                    String count = Integer.toString(toSend.length);
                    String toSendString = String.join(" ", toSendStringArr);

                    forRebalance.remove(file);
                    sendCount++;
                    sendString.append(" ").append(file).append(" ").append(count).append(" ").append(toSendString);
                }

                if (fileSizes.containsKey(file)) {
                    removeCount++;
                    removeString.append(" ").append(file);
                    fileDictionary.remove(file);
                    forRebalanceRemove.remove(file);
                } else if (forRebalanceRemove.get(file) > 0 && forRebalanceRemove.containsKey(file)) {
                    forRebalanceRemove.put(file, forRebalanceRemove.get(file) - 1);
                    removeCount++;
                    removeString.append(" ").append(file);

                    if (fileDictionary.containsKey(file) && fileDictionary.get(file).contains(port))
                        fileDictionary.get(file).remove(port);

                }
            }

            if (!temporaryPorts.containsKey(port)) {
                temporaryPorts.put(port, new ArrayList<>());
            }
            temporaryPorts.get(port).addAll(temporaryFiles);
            String message = " " + sendCount + sendString + " " + removeCount + removeString;

            try {
                Socket rebSocket= portMap.get(port);
                PrintWriter outRebalance = new PrintWriter(rebSocket.getOutputStream(), true);
                logger.logMessageSent(rebSocket,NetworkProtocol.REBALANCE + message);
                outRebalance.println(NetworkProtocol.REBALANCE + message);

            } catch (IOException e) {
                logger.logError("Rebalance Failed" + e);
            }
        }
        for(Integer port : temporaryPorts.keySet())
            portDictionary.get(port).addAll(temporaryPorts.get(port));

    }

    public void waitRebalance(){
        while(isRebalancing){
            continue;
        }
    }
}

