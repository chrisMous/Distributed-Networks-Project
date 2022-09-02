import java.net.Socket;

//The Logger class handles all of the relevant logging to the system used to display important events
public class Logger{
    public void logMessageSent(Socket socket,String msg){
        System.out.println("Message Sent: " + msg + "\n"
                + "From: " + socket.getLocalPort() + "\n"
                + "To: " + socket.getPort());
    }

    public void logMessageReceived(Socket socket,String msg){
        System.out.println("Message Received: " + msg + "\n"
                + "From: " + socket.getPort() + "\n"
                + "To: " + socket.getLocalPort());
    }

    public void logError(String msg){
        System.err.println(msg);
    }

    public void logDStore(Socket socket,int port){
        System.out.println("New DStore joined! " + "DStorePort : " + port + "\n"
                + "From: " + socket.getPort() + "\n" + "To: " + socket.getLocalPort());
    }

    public void log(String msg){
        System.out.println(msg);
    }
}
