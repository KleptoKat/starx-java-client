package com.kozakatak;

import java.io.*;
import java.net.Socket;
import java.util.*;

import com.google.gson.*;

/**
 * Created by Kozak on 2017-06-26.
 */
public class Session implements Runnable {


    public static final int RES_OK = 200;
    public static final int RES_FAIL = 500;
    public static final int RES_OLD_CLIENT = 501;

    public static final int TYPE_HANDSHAKE = 1;
    public static final int TYPE_HANDSHAKE_ACK = 2;
    public static final int TYPE_HEARTBEAT = 3;
    public static final int TYPE_DATA = 4;
    public static final int TYPE_KICK = 5;

    public static final int TYPE_REQUEST = 0;
    public static final int TYPE_NOTIFY = 1;
    public static final int TYPE_RESPONSE = 2;
    public static final int TYPE_PUSH = 3;

    public static final int PKG_HEAD_BYTES = 4;
    public static final int MSG_FLAG_BYTES = 1;
    public static final int MSG_ROUTE_CODE_BYTES = 2;
    public static final int MSG_ID_MAX_BYTES = 5;
    public static final int MSG_ROUTE_LEN_BYTES = 1;

    public static final int MSG_ROUTE_CODE_MAX = 0xffff;

    public static final int MSG_COMPRESS_ROUTE_MASK = 0x1;
    public static final int MSG_TYPE_MASK = 0x7;

    public static final String CLIENT_TYPE = "JAVA_KAT";
    public static final String CLIENT_VERSION = "0.0.1";

    public static final int RECONNECT_TIME_MS = 1000;

    private int heartbeatInterval = 0;
    private int heartbeatTimeoutInterval = 0;
    private boolean heartbeatAwaitingSend = false;
    private boolean awaitingHeartbeatFromServer = false;
    private Timer heartbeatTimeoutTimer;
    private Timer heartbeatTimer;

    private long lastMessageTime = 0;

    public int maxReconnectAttempts = 10;

    private boolean connected = false;

    private String host;
    private int port;
    private Thread thread;

    private OutputStream out;
    private InputStream in;
    private PrintStream log;
    private Gson gson;

    public Session(String host, int port) {
        this.host = host;
        this.port = port; // 3250
        thread = new Thread(this);
        gson = new GsonBuilder().create();
        heartbeatTimer = new Timer();
        heartbeatTimeoutTimer = new Timer();
        log = System.out;
    }

    public void setLog(PrintStream s) {
        log = s;
    }

    public boolean connect() {

        log.println("Connecting...");

        try {
            Socket s = new Socket(this.host, this.port);
            out = s.getOutputStream();
            in = s.getInputStream();
            connected = performHandshake();
            if (!connected) {
                return false;
            }

            thread.start();
            log.println("Connected");

        } catch (IOException e) {
            log.println("Could not connect to " + host + ":" + String.valueOf(port));
            e.printStackTrace(log);
            return false;
        }

        return true;
    }


    public void close() {
        this.connected = false;
        heartbeatTimer.cancel();
        heartbeatTimeoutTimer.cancel();
        // todo: end timers.
    }

    @Override
    public void run() {

        try {
            while (connected) {
                int type = in.read();
                if (type == -1) {
                    connected = false;
                    log.println("End of stream reached.");
                    break;
                }

                int length = in.read() << 16 | in.read() << 8 | in.read();
                byte[] bytes = new byte[length];

                in.read(bytes, 0, length);
                JsonElement raw = new JsonParser().parse(new String(bytes));
                JsonObject obj = null;
                if (raw.isJsonObject()) {
                    obj = (JsonObject)raw;
                }

                processPackage(type, obj);
                Thread.yield();
            }
        } catch (IOException e) {
            e.printStackTrace(log);
        }

        try {
            in.close();
            out.close();
        } catch (IOException e) {
            e.printStackTrace(log);
        }
    }

    private void processPackage(int type, JsonObject obj) {
        switch (type) {
            case TYPE_HANDSHAKE:
                processHandshake(obj);
                break;
            case TYPE_HEARTBEAT:
                processHeartbeat();
                break;
            case TYPE_DATA:
                break;
            case TYPE_KICK:
                break;
        }

        lastMessageTime = System.currentTimeMillis();
    }

    private void processHandshake(JsonObject obj) {
        JsonPrimitive codeElement = obj.getAsJsonPrimitive("code");
        if (codeElement == null || !codeElement.isNumber()) {
            return;
        }

        int code = codeElement.getAsInt();

        if (code == RES_OLD_CLIENT) {
            log.println("Error, old client");
            // TODO: error handling on connect.
            return;
        }

        if (code != RES_OK) {
            log.println("Error, handshake fail");
            return;
        }

        JsonObject sys = obj.getAsJsonObject("sys");
        JsonPrimitive heartbeat;
        if (sys != null) {
            heartbeat = sys.getAsJsonPrimitive("heartbeat");
            if (heartbeat != null && heartbeat.isNumber()) {
                heartbeatInterval = heartbeat.getAsInt() * 1000;
                heartbeatTimeoutInterval = heartbeatInterval * 2;
            }
        }

        // do handshake callback here
        send(TYPE_HANDSHAKE_ACK, new byte[] {});

    }

    private void processHeartbeat() {
        if (heartbeatInterval == 0) {
            // heartbeat disabled.
            return;
        }

        awaitingHeartbeatFromServer = false;

        if (heartbeatAwaitingSend) {
            // already in a heartbeat interval
            return;
        }

        heartbeatAwaitingSend = true;
        heartbeatTimer.cancel();
        heartbeatTimer.purge();
        heartbeatTimer = new Timer();
        heartbeatTimer.schedule(new TimerTask() {
            @Override
            public void run() {

                heartbeatAwaitingSend = false;
                send(TYPE_HEARTBEAT, new byte[] {});

                heartbeatTimeoutTimer.cancel();
                heartbeatTimeoutTimer.purge();
                heartbeatTimeoutTimer = new Timer();
                heartbeatTimeoutTimer.schedule(new TimerTask() {
                    @Override
                    public void run() {
                        if (awaitingHeartbeatFromServer &&
                                lastMessageTime + heartbeatTimeoutInterval - 100 < System.currentTimeMillis()) {
                            log.println("The server timed out.");
                            close();
                        }
                    }
                }, heartbeatTimeoutInterval);
            }
        }, heartbeatInterval);


    }



    private JsonElement getHandshake() {
        JsonObject sys = new JsonObject();
        sys.addProperty("type", CLIENT_TYPE);
        sys.addProperty("version", CLIENT_TYPE);

        JsonObject rsa = new JsonObject();
        sys.add("rsa", rsa);

        //JsonObject user = new JsonObject();


        JsonObject handshake = new JsonObject();
        handshake.add("sys", sys);
        //handshake.add("user", user);

        return handshake;
    }

    public boolean send(int type, byte[] bytes) {
        try {
            out.write(encode(type, bytes));
            out.flush();

        } catch (IOException e) {
            e.printStackTrace(log);
            return false;
        }

        return true;
    }

    public boolean send(int type, JsonElement element) {
        try {
            byte[] bytes = gson.toJson(element).getBytes();
            return send(type, bytes);
        } catch (JsonSyntaxException e) {
            e.printStackTrace(log);
        }
        return false;
    }

    private boolean performHandshake() {
        return send(TYPE_HANDSHAKE, getHandshake());
    }


    /**
     * Package protocol encode.
     *
     * Pomelo package format:
     * +------+-------------+------------------+
     * | type | body length |       body       |
     * +------+-------------+------------------+
     *
     * Head: 4bytes
     *   0: package type,
     *      1 - handshake,
     *      2 - handshake ack,
     *      3 - heartbeat,
     *      4 - data
     *      5 - kick
     *   1 - 3: big-endian body length
     * Body: body length bytes
     */
    private static byte[] encode(int type, byte[] body) {
        int length = body.length;
        byte[] buffer = new byte[length + PKG_HEAD_BYTES];
        int index = 0;
        buffer[index++] = (byte)(type & 0xff);
        buffer[index++] = (byte)((length >> 16) & 0xff);
        buffer[index++] = (byte)((length >> 8) & 0xff);
        buffer[index++] = (byte)(length & 0xff);

        if(length > 0) {
            System.arraycopy(buffer, index, body, 0, length);
        }
        return buffer;
    }

    private static ArrayList<IncomingMessage> decode(byte[] buffer) {
        int offset = 0;
        byte[] bytes = buffer.clone();
        int length = 0;
        ArrayList<IncomingMessage> messages = new ArrayList<>();
        while(offset < bytes.length) {

            int type = bytes[offset++];
            length = ((bytes[offset++]) << 16 | (bytes[offset++]) << 8 | bytes[offset++]);
            byte[] body = length > 0 ? new byte[length] : null;

            System.arraycopy(body, 0, bytes, offset, length);
            offset += length;

            JsonObject obj = (JsonObject) (new JsonParser().parse(new String(bytes)));
            IncomingMessage msg = new IncomingMessage(type, obj);
            messages.add(msg);
        }
        return messages;
    }

    static class IncomingMessage {


        private int type;
        private JsonObject body;

        public IncomingMessage(int type, JsonObject body) {
            this.type = type;
            this.body = body;
        }


        public int getType() {
            return type;
        }

        public JsonObject getBody() {
            return body;
        }



    }

}
