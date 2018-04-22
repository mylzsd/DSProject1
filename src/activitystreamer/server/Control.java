package activitystreamer.server;

import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import activitystreamer.util.Settings;

public class Control extends Thread {
	private static final Logger log = LogManager.getLogger();
	private static ArrayList<Connection> connections;
	private static boolean term = false;
	private static Listener listener;
    private JSONParser parser = new JSONParser();
	
	protected static Control control = null;
	
	public static Control getInstance() {
		if(control == null){
			control = new Control();
		} 
		return control;
	}
	
	public Control() {
		// initialize the connections array
		connections = new ArrayList<Connection>();
		// start a listener
		try {
			listener = new Listener();
		} catch (IOException e1) {
			log.fatal("failed to startup a listening thread: " + e1);
			System.exit(-1);
		}	
	}
	
	public void initiateConnection() {
		// make a connection to another server if remote hostname is supplied
		if (Settings.getRemoteHostname() != null) {
			try {
				outgoingConnection(new Socket(Settings.getRemoteHostname(), Settings.getRemotePort()));
			} catch (IOException e) {
				log.error("failed to make connection to " + Settings.getRemoteHostname() + ":" + Settings.getRemotePort() + " :" + e);
				System.exit(-1);
			}
		}
	}
	
	/*
	 * Processing incoming messages from the connection.
	 * Return true if the connection should close.
	 */
	public synchronized boolean process(Connection con, String msg) {
	    JSONObject requestObj;
	    String command;
	    JSONObject responseObj = new JSONObject();
        responseObj.put("command", "INVALID_MESSAGE");
	    try {
	        requestObj = (JSONObject) parser.parse(msg);
	        command = (String) requestObj.get("command");
	        if (command == null) {
                log.error("the received message did not contain a command");
                responseObj.put("info", "the received message did not contain a command");
                con.writeMsg(responseObj.toString());
                return true;
            }
        } catch (ParseException e) {
            log.error("JSON parse error while parsing message");
            responseObj.put("info", "JSON parse error while parsing message");
            con.writeMsg(responseObj.toString());
            return true;
        }

        switch (command) {
	        // Server communication part
            case "AUTHENTICATE":
                break;
            case "SERVER_ANNOUNCE":
                break;
            case "ACTIVITY_BROADCAST":
                break;
            case "LOCK_REQUEST":
                break;
            // Client part
            case "REGISTER":
                break;
            case "LOGIN":
                break;
            case "LOGOUT":
                break;
            case "ACTIVITY_MESSAGE":
                break;
            // Request does not contain a command field
            default:
                log.error("the received message contains a invalid command");
                responseObj.put("info", "the received message contains a invalid command");
                con.writeMsg(responseObj.toString());
                return true;
        }
		return false;
	}
	
	/*
	 * The connection has been closed by the other party.
	 */
	public synchronized void connectionClosed(Connection con) {
		if (!term) connections.remove(con);
	}
	
	/*
	 * A new incoming connection has been established, and a reference is returned to it
	 */
	public synchronized Connection incomingConnection(Socket s) throws IOException {
		log.debug("incoming connection: " + Settings.socketAddress(s));
		Connection c = new Connection(s);
		connections.add(c);
		return c;
		
	}
	
	/*
	 * A new outgoing connection has been established, and a reference is returned to it
	 */
	public synchronized Connection outgoingConnection(Socket s) throws IOException{
		log.debug("outgoing connection: " + Settings.socketAddress(s));
		Connection c = new Connection(s);
		c.setType(1);
		connections.add(c);
		return c;
		
	}
	
	@Override
	public void run() {
		log.info("using activity interval of " + Settings.getActivityInterval() + " milliseconds");
		while (!term) {
			// do something with 5 second intervals in between
			try {
				Thread.sleep(Settings.getActivityInterval());
			} catch (InterruptedException e) {
				log.info("received an interrupt, system is shutting down");
				break;
			}
			if (!term) {
				log.debug("doing activity");
				term = doActivity();
			}
			
		}
		log.info("closing " + connections.size() + " connections");
		// clean up
		for(Connection connection : connections){
			connection.closeCon();
		}
		listener.setTerm(true);
	}
	
	public boolean doActivity() {
		return false;
	}
	
	public final void setTerm(boolean t) {
		term = t;
	}
	
	public final ArrayList<Connection> getConnections() {
		return connections;
	}
}
