package activitystreamer.client;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
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
    private BufferedWriter out = null;
    private JSONParser parser = new JSONParser();
    private boolean loginSuccess = false;
	
	public static ClientSkeleton getInstance() {
		if (clientSolution==null) {
			clientSolution = new ClientSkeleton();
		}
		return clientSolution;
	}
	
	public ClientSkeleton() {
        // Try register using given username and secret
        int attemptCount = 1;
        boolean registerSuccess = false;
        while (!registerSuccess) {
            if (attemptCount > MAX_ATTEMPT) {
                log.fatal("Maximum attempts exceeded");
                System.exit(-1);
            }
            log.info(String.format("Trying to register user to %s:%d, attempt %d",
                    Settings.getRemoteHostname(), Settings.getRemotePort(), attemptCount++));
            try {
                JSONObject response = register();
                String command = (String) response.get("command");
                switch (command) {
                    case "REGISTER_FAILED":
                        log.info("User already registered, ready to login");
                        registerSuccess = true;
                        break;
                    case "REGISTER_SUCCESS":
                        log.info("Register success, ready to login");
                        registerSuccess = true;
                        break;
                    case "INVALID_MESSAGE":
                        log.error("Request format error");
                        break;
                    default:
                        log.error("Command not found in response");
                }
                if (!command.equals("REGISTER_SUCCESS")) socket.close();
            } catch (Exception e) {

            }
        }
        /*
            Try login
            if (LOGIN_SUCCESS) start listening or broadcasting
            else if REDIRECT setup new parameters and retry login
            else if (LOGIN_FAILED) log fatal and exit
            else if INVALID_MESSAGE log error
        */
        attemptCount = 1;
        while (!loginSuccess) {
            if (attemptCount > MAX_ATTEMPT) {
                log.fatal("Maximum attempts exceeded");
                System.exit(-1);
            }
            log.info(String.format("Trying to login user to %s:%d, attempt %d",
                    Settings.getRemoteHostname(), Settings.getRemotePort(), attemptCount++));
            try {
                JSONObject response = login();
                String command = (String) response.get("command");
                switch (command) {
                    case "LOGIN_SUCCESS":
                        log.info("Login success!!");
                        if (in.ready()) {
                            response = (JSONObject) parser.parse(in.readLine());
                            if (response != null) {
                                String cmd = (String) response.get("command");
                                if (cmd.equals("REDIRECT")) {
                                    if (response.containsKey("hostname") && response.containsKey("port")) {
                                        Settings.setRemoteHostname((String) response.get("hostname"));
                                        Settings.setRemotePort(((Long) response.get("port")).intValue());
                                        socket.close();
                                        attemptCount = 1;
                                    }
                                }
                            }
                        }
                        else {
                            loginSuccess = true;
                        }
                        break;
                    case "REDIRECT":
                        if (response.containsKey("hostname") && response.containsKey("port")) {
                            Settings.setRemoteHostname((String) response.get("hostname"));
                            Settings.setRemotePort((int) response.get("port"));
                            attemptCount = 1;
                        }
                        else {
                            log.error("Cannot find hostname or port in response");
                        }
                        break;
                    case "LOGIN_FAILED":
                        log.fatal((String) response.get("info"));
                        System.exit(-1);
                        break;
                    case "INVALID_MESSAGE":
                        log.error("Request format incorrect");
                        break;
                    default:
                        log.error("Command not found in response");
                }
                if (!command.equals("LOGIN_SUCCESS")) socket.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        textFrame = new TextFrame();
		start();
	}
	
	private void setupConnection() throws IOException {
	    try {
            socket = new Socket(Settings.getRemoteHostname(), Settings.getRemotePort());
        } catch (IllegalArgumentException e) {
	        log.fatal("Illegal port number is used");
	        System.exit(-1);
        } catch (NullPointerException e) {
            log.fatal("Host address cannot be empty");
            System.exit(-1);
        }
        in = new BufferedReader(new InputStreamReader(socket.getInputStream(), "UTF-8"));
        out = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), "UTF-8"));
    }
	
	public JSONObject register() throws IOException, ParseException {
	    if (socket == null || socket.isClosed())
	        setupConnection();
		JSONObject requestObj = new JSONObject();
        requestObj.put("command", "REGISTER");
        requestObj.put("username", Settings.getUsername());
        requestObj.put("secret", Settings.getSecret());
        String request = requestObj.toString() + "\n";

        String response = send(request, true);
        JSONObject ret = (JSONObject) parser.parse(response);
        return ret;
    }

    public JSONObject login() throws IOException, ParseException {
        if (socket == null || socket.isClosed())
            setupConnection();
        JSONObject user = new JSONObject();
        user.put("command", "LOGIN");
        user.put("username", Settings.getUsername());
        user.put("secret", Settings.getSecret());
        String string = user.toString() + "\n";
        String response = send(string, true);
        JSONObject result = (JSONObject) parser.parse(response);
        return result;
    }

	public void sendActivityObject(JSONObject activityObj) {
	    if (socket.isClosed()) {
	        log.error("Connection closed.");
	        return;
        }
        JSONObject requestObj = new JSONObject();
        requestObj.put("command", "ACTIVITY_MESSAGE");
        requestObj.put("username", Settings.getUsername());
        requestObj.put("secret", Settings.getSecret());
        requestObj.put("activity", activityObj);
        String request = requestObj.toString() + "\n";
        try {
            send(request, false);
        } catch (IOException e) {
            log.error("Failed to send activity message");
        }
	}
	
	
	public void disconnect() {
	    if (!socket.isClosed()) {
            loginSuccess = false;
            try {
                JSONObject requestObj = new JSONObject();
                requestObj.put("command", "LOGOUT");
                String request = requestObj.toString() + "\n";
                send(request, false);
                socket.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
	}

	private String send(String request, boolean response) throws IOException {
        out.write(request);
        out.flush();
        if (response)
            return in.readLine();
        return null;
    }
	
	public void run() {
        while (loginSuccess) {
            try {
                String response = in.readLine();
                if (response != null) {
                    // System.out.println("reading: " + response);
                    JSONObject responseObj = (JSONObject) parser.parse(response);
                    String command = (String) responseObj.get("command");
                    if (command.equals("ACTIVITY_BROADCAST"))
                        textFrame.setOutputText((JSONObject) responseObj.get("activity"));
                }
            } catch (Exception e) {
                // e.printStackTrace();
            }
        }
	}
}
