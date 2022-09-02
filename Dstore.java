


import java.io.*;
import java.net.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Objects;

public class Dstore {
    private final Logger logger;
    private final Integer port;
    private final Integer cport;
    private final Integer timeout;
    private final String fileFolder;
    private File fileDir;
    private Socket controllerSocket;
    private Socket clientSocket;

    private final ServerSocket serverSocket;
    private String[] inputs;
    private String[] clientInputs;
    private String dirPath;
    private boolean failure = false;
    public Dstore(int port, int cport, int timeout, String fileFolder) throws IOException {
        this.port = port;
        this.cport = cport;
        this.timeout = timeout;
        this.fileFolder = fileFolder;
        this.logger = new Logger();
        serverSocket = new ServerSocket(port);

        try {
            this.initialise();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException {
        Dstore dstore = new Dstore(Integer.parseInt(args[0]), Integer.parseInt(args[1]), Integer.parseInt(args[2]), args[3]);
    }

    public void makeFileDir(String fileFolder){
        this.fileDir = new File(fileFolder);


        if(!this.fileDir.exists()){
            this.fileDir.mkdir();
        }
        else this.logger.logError("File Directory Already Exists");

        this.dirPath = fileDir.getAbsolutePath();


        for (File file : Objects.requireNonNull(fileDir.listFiles()))
        {
            file.delete();
        }
    }

    public void initialise() throws IOException{
        makeFileDir(this.fileFolder);

        controllerSocket = new Socket(InetAddress.getByName("localhost"), cport);
        initialiseControllerThread(controllerSocket);

        if(failure)
            return;

        try {
            while(true) {
                initialiseClientThread(controllerSocket);
            }
        } catch (IOException e) {
            logger.logError("SERVER SOCKET ERROR");
            e.printStackTrace();
        }

    }

    private void initialiseControllerThread(Socket controllerSocket){
        new Thread(() -> {
            try {
                PrintWriter transmitter = new PrintWriter(controllerSocket.getOutputStream(), true);
                BufferedReader receiver = new BufferedReader(new InputStreamReader(controllerSocket.getInputStream()));

                transmitter.println(NetworkProtocol.JOIN + " " + this.port);
                logger.logMessageSent(controllerSocket,"JOIN");
                String in;
                try{
                    while(true){

                         in = receiver.readLine();
                        logger.logMessageReceived(controllerSocket,in);

                        if(in != null){
                            String output;
                            inputs = in.split(" ");

                            if(inputs.length != 1){
                                output = inputs[0];
                                inputs[inputs.length - 1] = inputs[inputs.length - 1].trim();
                            }
                            else {
                                output = in.trim();
                                inputs[0] = output;
                            }
                            logger.log("CONTROLLER RECEIVED: " + output);
                            handleControllerString(output,transmitter,receiver);
                        }
                        else {
                            if(controllerSocket.isConnected()){
                                logger.logError("CONTROLLER CLOSED");
                                controllerSocket.close();
                                this.failure = true;
                            }
                        }

                    }
                } catch (Exception e) {
                    if(controllerSocket.isConnected()){
                        controllerSocket.close();
                        this.failure = true;
                    }
                    e.printStackTrace();
                }


            } catch (IOException e) {
                e.printStackTrace();
                this.failure = true;
                logger.logError("CONTROLLER ERROR");
            }
        }).start();
    }


    private void handleControllerString(String message, PrintWriter transmitter, BufferedReader receiver) throws IOException {
        switch (message) {
            case NetworkProtocol.LIST:
                if (inputs.length == 1) {
                    String[] list = fileDir.list();
                    String listToSend = String.join(" ", list);

                    transmitter.println(NetworkProtocol.LIST + " " + listToSend);

                    logger.logMessageSent(controllerSocket, NetworkProtocol.LIST + " " + listToSend);

                } else logger.logError("Incorrect LIST message format");
                break;
            case NetworkProtocol.REMOVE:
                if (inputs.length == 2) {
                    String filename = inputs[1];
                    File fileToRemove = new File(dirPath + File.separator + filename);

                    if (fileToRemove.exists() && fileToRemove.isFile()) {
                        fileToRemove.delete();
                        transmitter.println(NetworkProtocol.REMOVE_ACK + " " + filename);
                        logger.logMessageSent(controllerSocket, NetworkProtocol.REMOVE_ACK + " " + filename);
                    } else {
                        transmitter.println(NetworkProtocol.FILE_DOES_NOT_EXIST + " " + filename);
                        logger.logError(NetworkProtocol.FILE_DOES_NOT_EXIST + " " + filename);
                    }

                } else logger.logError("Incorrect REMOVE message format");
                break;
            case NetworkProtocol.REBALANCE:

                int noOfFiles = Integer.parseInt(inputs[1]);
                int pointer = 2;

                for (int i = 0; i < noOfFiles; i++) {
                    String filename = inputs[pointer];
                    int portsToSend = Integer.parseInt(inputs[pointer + 1]);

                    for (int j = pointer + 2; j <= pointer + 1 + portsToSend; j++) {
                        Socket socket = new Socket(InetAddress.getByName("localhost"), Integer.parseInt(inputs[j]));

                        BufferedReader in= new BufferedReader(new InputStreamReader(socket.getInputStream()));
                        PrintWriter out = new PrintWriter(socket.getOutputStream(), true);

                        File f = new File(dirPath + File.separator + filename);
                        int size = (int) f.length();

                        out.println(NetworkProtocol.REBALANCE_STORE + " " + filename + " " + size);
                        logger.logMessageSent(socket, NetworkProtocol.REBALANCE_STORE + " " + filename + " " + size);

                        if (in.readLine().equals(NetworkProtocol.ACK)) {
                            logger.logMessageReceived(socket, NetworkProtocol.ACK);

                            FileInputStream fileStream = new FileInputStream(f);
                            OutputStream outS = socket.getOutputStream();
                            outS.write(fileStream.readNBytes(size));
                            outS.flush();
                            fileStream.close();
                            outS.close();
                        }

                        socket.close();
                    }
                    pointer += portsToSend + 2;

                }
                int rm = Integer.parseInt(inputs[pointer]);

                for (int x = pointer + 1; x < pointer + 1 + rm; x++) {
                    File fileToRemove = new File(dirPath + File.separator + inputs[x]);

                    if (fileToRemove.exists())
                        fileToRemove.delete();

                }

                transmitter.println(NetworkProtocol.REBALANCE_COMPLETE);
                logger.logMessageSent(controllerSocket, NetworkProtocol.REBALANCE_COMPLETE);
                break;
            default:
                logger.logError("Incorrect Message Received");
                break;
        }
    }


    private void initialiseClientThread(Socket controllerSocket) throws IOException {
        clientSocket = serverSocket.accept();

        new Thread(() -> {
            try {
                PrintWriter clientTransmitter = new PrintWriter(clientSocket.getOutputStream(), true);
                BufferedReader clientReceiver = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                InputStream input = clientSocket.getInputStream();
                String in;
                PrintWriter transmitter = new PrintWriter(controllerSocket.getOutputStream(), true);

                while(true) {
                    try {
                        try {
                             in = clientReceiver.readLine();

                            if(in != null){
                                String output;
                                clientInputs = in.split(" ");

                                if(clientInputs.length != 1){
                                     output = clientInputs[0];
                                }

                                else {
                                    output = in.trim();
                                    clientInputs[0] = output;
                                }
                                logger.log("CLIENT RECEIVED: " + output);
                                handleClientString(output,clientTransmitter,input,transmitter);
                            }
                            else{
                                clientSocket.close();
                                break;
                            }
                        } catch (IOException e) {
                            logger.logError("CLIENT DISCONNECTED");
                            clientSocket.close();
                        }

                    } catch (Exception e) {
                        clientSocket.close();
                        logger.logError("DSTORE CLIENT ERROR");
                        break;
                    }
                }

            } catch (IOException e) {
                e.printStackTrace();
                logger.logError("DSTORE Crashed");
            }
        }).start();
    }

    private void handleClientString(String message, PrintWriter clientTransmitter, InputStream input, PrintWriter transmitter) throws IOException {
        switch (message) {
            case NetworkProtocol.STORE:
                if (clientInputs.length == 3) {
                    logger.logMessageSent(clientSocket, NetworkProtocol.ACK);
                    clientTransmitter.println(NetworkProtocol.ACK);

                    File fileToSTore = new File(fileDir.getAbsolutePath() + File.separator + clientInputs[1]);
                    Path path = (Path) Paths.get(fileToSTore.getPath());
                    FileOutputStream fileToTransmitter = new FileOutputStream(path.toFile());
                    long maxTimeToWait = System.currentTimeMillis() + this.timeout;
                    int size = Integer.parseInt(clientInputs[2]);


                    while (maxTimeToWait >= System.currentTimeMillis()) {
                        fileToTransmitter.write(input.readNBytes(size));

                        logger.logMessageSent(clientSocket, NetworkProtocol.STORE_ACK + " " + clientInputs[1]);
                        transmitter.println(NetworkProtocol.STORE_ACK + " " + clientInputs[1]);
                        break;
                    }

                    fileToTransmitter.flush();
                    fileToTransmitter.close();
                    clientSocket.close();
                    return;

                } else logger.logError("Incorrect STORE message format");
                break;
            case NetworkProtocol.LOAD_DATA:
                if (clientInputs.length == 2) {

                    File fileToLoad= new File(fileDir.getAbsolutePath() + File.separator + clientInputs[1]);
                    Path path = (Path) Paths.get(fileToLoad.getPath());

                    BasicFileAttributes basicFileAttributes = Files.readAttributes(fileToLoad.toPath(), BasicFileAttributes.class);
                    if (fileToLoad.exists() && basicFileAttributes.isRegularFile()) {

                        long size = basicFileAttributes.size();
                        FileInputStream fileFromInput = new FileInputStream(path.toFile());
                        OutputStream fileTransmitter = clientSocket.getOutputStream();
                        fileTransmitter.write(fileFromInput.readNBytes((int) size));

                        fileTransmitter.flush();
                        fileFromInput.close();
                        fileTransmitter.close();

                    } else {
                        logger.log("Closing Client");
                    }
                    clientSocket.close();
                    return;


                } else logger.logError("Incorrect LOAD_DATA message format");
                break;
            case NetworkProtocol.REBALANCE_STORE:
                if (clientInputs.length == 3) {

                    logger.logMessageSent(clientSocket, NetworkProtocol.ACK);
                    clientTransmitter.println(NetworkProtocol.ACK);

                    int size = Integer.parseInt(clientInputs[2]);
                    File fileToTransmit = new File(dirPath + File.separator + clientInputs[1]);
                    Path path = (Path) Paths.get(fileToTransmit.getPath());
                    FileOutputStream fileTransmitter = new FileOutputStream(path.toFile());

                    fileTransmitter.write(input.readNBytes(size));
                    fileTransmitter.flush();
                    fileTransmitter.close();

                    logger.log("Closing Client");
                    clientSocket.close();
                    return;
                } else logger.logError("Incorrect REBALANCE_STORE message format");
                break;
            default:
                logger.logError("Incorrect Message Received");
                break;
        }

    }

}
