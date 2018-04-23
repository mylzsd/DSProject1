package activitystreamer.client;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import activitystreamer.util.Settings;

public class ClientSkeleton extends Thread {
    private static final int MAX_ATTEMPT = 5;
	private static final Logger log = LogManager.getLogger();
	private static ClientSkeleton clientSolution;
	private TextFrame textFrame;

	private Socket socket = null;
    private BufferedReader in = null;
    private PrintWriter out = null;
    private JSONParser parser = new JSONParser();
    private boolean term = false;
    private boolean open = false;
    private int redirectCount = 0;
	
	public static ClientSkeleton getInstance() {
		if (clientSolution == null) {
			clientSolution = new ClientSkeleton();
		}
		return clientSolution;
	}
	
	public ClientSkeleton() {
        setupConnection();
	    if (Settings.getSecret() == null) {
            // register with new secret
            String secret = Settings.nextSecret();
            System.out.println(String.format("Your new secret is %s", secret));
            Settings.setSecret(secret);
	        register();
        }
        else {
            // login with whatever in Settings
            login();
        }
        // start GUI and listener
        textFrame = new TextFrame();
		start();
	}

    public void sendActivityObject(JSONObject activityObj) {
        if (!open) {
            log.error("Connection closed");
            return;
        }
        JSONObject requestObj = new JSONObject();
        requestObj.put("command", "ACTIVITY_MESSAGE");
        requestObj.put("username", Settings.getUsername());
        requestObj.put("secret", Settings.getSecret());
        requestObj.put("activity", activityObj);
        send(requestObj.toString());
    }


    public void disconnect() {
	    term = true;
        if (open) logout();
        try {
            closeConnection();
        } catch (IOException e) {
            log.error("received exception when closing the connection: " + e);
        }
    }
	
	private void setupConnection() {
	    try {
            socket = new Socket(Settings.getRemoteHostname(), Settings.getRemotePort());
            in = new BufferedReader(new InputStreamReader(new DataInputStream(socket.getInputStream())));
            out = new PrintWriter(new DataOutputStream(socket.getOutputStream()), true);
            open = true;
        } catch (IllegalArgumentException e) {
	        log.fatal("Illegal port number is used");
	        System.exit(-1);
        } catch (NullPointerException e) {
            log.fatal("Host address cannot be empty");
            System.exit(-1);
        } catch (IOException e) {
	        log.fatal("Failed to establish connection with server");
	        System.exit(-1);
        }
    }

    private void closeConnection() throws IOException {
	    if (open) {
            log.info("closing connection with server...");
            in.close();
            out.close();
            socket.close();
            open = false;
        }
    }

    private void register() {
        JSONObject outObj = new JSONObject();
        outObj.put("command", "REGISTER");
        outObj.put("username", Settings.getUsername());
        outObj.put("secret", Settings.getSecret());
        send(outObj.toString());
    }

    private void login() {
        JSONObject outObj = new JSONObject();
        outObj.put("command", "LOGIN");
        outObj.put("username", Settings.getUsername());
        outObj.put("secret", Settings.getSecret());
        send(outObj.toString());
    }

    private void logout() {
        JSONObject outObj = new JSONObject();
        outObj.put("command", "LOGOUT");
        send(outObj.toString());
    }

    private boolean redirect(JSONObject obj) {
	    if (redirectCount++ >= MAX_ATTEMPT) {
	        log.fatal("Maximum redirect count exceeded");
	        return true;
        }
        String hostname = (String) obj.get("hostname");
	    Long port = (Long) obj.get("port");
        if (hostname == null) {
            log.fatal("Received message does not contain a hostname");
            invalidMessage("received message does not contain a hostname");
            return true;
        }
        if (port == null) {
            log.fatal("Received message does not contain a port");
            invalidMessage("received message does not contain a port");
            return true;
        }
        try {
            closeConnection();
        } catch (IOException e) {
            log.error("received exception when closing the connection: " + e);
            return true;
        }
        setupConnection();
        login();
	    return false;
    }

	private void invalidMessage(String info) {
	    JSONObject outObj = new JSONObject();
        outObj.put("command", "INVALID_MESSAGE");
        outObj.put("info", info);
	    send(outObj.toString());
    }

	private boolean send(String msg) {
	    if (open) {
            out.println(msg);
            out.flush();
            return true;
        }
        return false;
    }

    private boolean process(String msg) {
        JSONObject inObj;
        String command;
        try {
            inObj = (JSONObject) parser.parse(msg);
            command = (String) inObj.get("command");
            if (command == null) {
                log.error("the received message did not contain a command");
                invalidMessage("the received message did not contain a command");
                return true;
            }
        } catch (ParseException e) {
            log.error("JSON parse error while parsing message");
            invalidMessage("the message sent was not a valid json object");
            return true;
        }

        switch (command) {
            case "INVALID_MESSAGE":
                log.error(String.format("An invalid message is sent, info from another party: %s", inObj.get("info")));
                return true;
            case "REGISTER_SUCCESS":
                log.info("Register success");
                login();
                return false;
            case "REGISTER_FAILED":
                log.fatal(String.format("Register failed: %s", inObj.get("info")));
                return true;
            case "LOGIN_SUCCESS":
                log.info("Login success");
                return false;
            case "REDIRECT":
                log.info("Redirect");
                return redirect(inObj);
            case "LOGIN_FAILED":
                log.fatal((String) inObj.get("info"));
                return true;
            case "ACTIVITY_BROADCAST":
                log.info("Activity received");
                JSONObject activity = (JSONObject) inObj.get("activity");
                if (activity == null) {
                    log.error("Received message does not contain an activity");
                    invalidMessage("message does not contain an activity");
                    return true;
                }
                textFrame.setOutputText(activity);
                return false;
            default:
                log.error("the received message contains a invalid command");
                invalidMessage("the received message contains a invalid command");
                return true;
        }
    }
	
	public void run() {
        try {
            String response;
            while (!term && (response = in.readLine()) != null) {
                term = process(response);
            }
            log.debug("stop receiving message from server");
            socket.close();
        } catch (IOException e) {
            log.error("exit with exception: " + e);
        }
        System.exit(0);
	}
}
