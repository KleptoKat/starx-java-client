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

    private int heartbeatInterval = 0;
    private int heartbeatTimeoutInterval = 0;
    private boolean heartbeatAwaitingSend = false;
    private boolean awaitingHeartbeatFromServer = false;
    private Timer heartbeatTimeoutTimer;
    private Timer heartbeatTimer;

    private long lastMessageTime = 0;

    private boolean connected = false;

    private Map<String, MessageListener> routeCallbacks;
    private Map<Integer, MessageListener> requestCallbacks;

    private String host;
    private int port;
    private Thread thread;

    private OutputStream out;
    private InputStream in;
    private PrintStream log;
    private Gson gson;
    private int lastMessageID = 1;

    public Session(String host, int port) {
        this.host = host;
        this.port = port; // 3250
        thread = new Thread(this);
        gson = new GsonBuilder().create();
        heartbeatTimer = new Timer();
        heartbeatTimeoutTimer = new Timer();
        log = System.out;
        routeCallbacks = new HashMap<>();
        requestCallbacks = new HashMap<>();
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

                //noinspection ResultOfMethodCallIgnored
                in.read(bytes, 0, length);
                processPackage(type, bytes);
                Thread.yield();
            }
        } catch (IOException e) {
            fatalError(e);
        }

        try {
            in.close();
            out.close();
        } catch (IOException e) {
            e.printStackTrace(log);
        }
    }



    ///////////////////////////////////////////////////////////////////////
    ///////////////////////////// PROCESSORS //////////////////////////////
    ///////////////////////////////////////////////////////////////////////

    private void processPackage(int type, byte[] bytes) {
        switch (type) {
            case TYPE_HANDSHAKE:
                processHandshake(bytes);
                break;
            case TYPE_HEARTBEAT:
                processHeartbeat();
                break;
            case TYPE_DATA:
                processMessage(bytes);
                break;
            case TYPE_KICK:
                processKick(bytes);
                break;
        }

        lastMessageTime = System.currentTimeMillis();
    }

    private void processHandshake(byte[] bytes) {

        JsonElement element = (new JsonParser()).parse(new String(bytes));
        if (!element.isJsonObject()) {
            return;
        }

        JsonObject obj = element.getAsJsonObject();

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

    private void processMessage(byte[] bytes) {

        IncomingMessage msg = decodeMessage(bytes);
        if (msg == null) {
            return;
        }

        if (msg.isIDType()) {
            MessageListener l = requestCallbacks.get(msg.getId());
            if (l != null) {
                l.call(msg.getObject());
                requestCallbacks.remove(msg.getId());
                log.println(msg.getId());
            } else {
                log.println("Invalid response with id " + msg.getId());
            }
        }

        if (msg.isRouteType()) {
            MessageListener l = routeCallbacks.get(msg.getRoute());
            if (l != null) {
                l.call(msg.getObject());
                routeCallbacks.remove(msg.getRoute());
            } else {
                log.println("Invalid route.");
            }
        }
    }

    private void processKick(byte[] bytes) {
        // TODO: onKick
    }

    ///////////////////////////////////////////////////////////////////////
    /////////////////////////////// SENDERS ///////////////////////////////
    ///////////////////////////////////////////////////////////////////////


    public void notify(String route, MessageListener cb) { notify(route, new JsonObject(), cb); }
    public void notify(String route, JsonObject data, MessageListener cb) {
        byte[] msg = encodeMessage(0, TYPE_REQUEST, route, data);
        if (msg != null) {
            send(TYPE_DATA, msg);
        }
    }


    public void request(String route, MessageListener cb) { request(route, new JsonObject(), cb); }
    public void request(String route, JsonObject data, MessageListener cb) {
        int id = lastMessageID++;

        byte[] msg = encodeMessage(id, TYPE_REQUEST, route, data);
        if (msg != null) {
            requestCallbacks.put(id, cb);
            send(TYPE_DATA, msg);
        }
    }


    ///////////////////////////////////////////////////////////////////////
    ///////////////////////////// ELEMENTARY //////////////////////////////
    ///////////////////////////////////////////////////////////////////////


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

    private boolean performHandshake() {
        return send(TYPE_HANDSHAKE, getHandshake());
    }

    private boolean send(int type, byte[] bytes) {
        try {
            out.write(encode(type, bytes));
            out.flush();

        } catch (IOException e) {
            fatalError(e);

            return false;
        }

        return true;
    }

    private boolean send(int type, JsonElement element) {
        return send(type, gson.toJson(element).getBytes());
    }

    ///////////////////////////////////////////////////////////////////////
    ////////////////////////////// HANDLERS ///////////////////////////////
    ///////////////////////////////////////////////////////////////////////

    private void fatalError(Exception e) {
        e.printStackTrace();
    }


    public void on(String route, MessageListener l) {
        if (routeCallbacks.get(route) != null) {
            log.println("There is a duplicate entry of route " + route);
        }

        routeCallbacks.put(route, l);
    }

    ///////////////////////////////////////////////////////////////////////
    ////////////////////////////// PROTOCOLS //////////////////////////////
    ///////////////////////////////////////////////////////////////////////

    private boolean msgHasId(int type) {
        return type == TYPE_REQUEST || type == TYPE_RESPONSE;
    }

    private boolean msgHasRoute(int type) {
        return type == TYPE_REQUEST || type == TYPE_PUSH || type == TYPE_NOTIFY;
    }

    private int calculateMsgIDBytes(int id) {
        int len = 0;
        do {
            len += 1;
            id >>= 7;
        } while(id > 0);
        return len;
    }

    private IncomingMessage decodeMessage(byte[] buffer) {
        byte[] bytes =  buffer.clone();
        int offset = 0;
        int id = -1;
        String route = null;

        // parse flag
        byte flag = bytes[offset++];
        byte type = (byte)((flag >> 1) & MSG_TYPE_MASK);



        // parse id
        if(msgHasId(type)) {
            int m;
            int i = 0;
            do{
                m = bytes[offset++];
                id = (int)(id + ((m & 0x7f) * Math.pow(2,(7*i++)))) + 1;
            } while(m >= 128);
        }

        // parse route
        if(msgHasRoute(type)) {
            int routeLen = bytes[offset++];
            if(routeLen > 0) {
                byte[] bRoute = new byte[routeLen];
                System.arraycopy(bytes, offset, bRoute, 0, routeLen);
                route = new String(bRoute);
            } else {
                route = null;
            }
            offset += routeLen;
        }

        // parse body
        int bodyLen = bytes.length - offset;
        byte[] body = new byte[bodyLen];
        System.arraycopy(bytes, offset, body, 0, bodyLen);

        JsonElement element = (new JsonParser()).parse(new String(body));
        if (!element.isJsonObject()) {
            return null;
        }

        if (id >= 0) {
            return new IncomingMessage(type, id, (JsonObject)element);
        }


        if (route != null) {
            return new IncomingMessage(type, route, (JsonObject)element);
        }

        return null;

    }

    private byte[] encodeMessage(int id, int type, String route, JsonObject msg) {

        // message type has an id
        int msgLen = MSG_FLAG_BYTES;
        if (type == TYPE_REQUEST || type == TYPE_RESPONSE) {
            msgLen += calculateMsgIDBytes(id);
        }

        // message type has a route
        byte[] bRoute = new byte[0];
        if (type == TYPE_REQUEST || type == TYPE_PUSH || type == TYPE_NOTIFY) {
            msgLen += MSG_ROUTE_LEN_BYTES;
            if (route != null) {
                bRoute = route.getBytes();
                if (bRoute.length > 255) {
                    log.println("Route is too long.");
                    return null;
                } else {
                    msgLen += bRoute.length;
                }
            }
        }

        byte[] bMsg = new byte[0];
        if (msg != null) {
            bMsg = gson.toJson(msg).getBytes();
            msgLen += bMsg.length;
        }

        byte[] buffer = new byte[msgLen];
        int offset = 0;

        // write flag to buffer
        buffer[offset] = (byte) ((type << 1) & 0xFF);
        offset = offset + MSG_FLAG_BYTES;


        // write id to bugger
        if (type == TYPE_REQUEST || type == TYPE_RESPONSE) {
            do{
                int tmp = id % 128;
                int next = (int)Math.floor(id/128);

                if(next != 0){
                    tmp = tmp + 128;
                }
                buffer[offset++] = (byte) (tmp & 0xFF);

                id = next;
            } while(id != 0);
        }

        // write route to buffer
        if (type == TYPE_REQUEST || type == TYPE_PUSH || type == TYPE_NOTIFY) {
            if(bRoute.length > 0) {
                buffer[offset++] = (byte) (bRoute.length & 0xff);
                for (byte aBRoute : bRoute) {
                    buffer[offset++] = aBRoute;
                }
            } else {
                buffer[offset++] = 0;
            }
        }

        // write body to buffer
        for (byte aBMsg : bMsg) {
            buffer[offset++] = aBMsg;
        }


        return buffer;
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
    private byte[] encode(int type, byte[] body) {
        int length = body.length;
        byte[] buffer = new byte[length + PKG_HEAD_BYTES];
        int index = 0;
        buffer[index++] = (byte)(type & 0xff);
        buffer[index++] = (byte)((length >> 16) & 0xff);
        buffer[index++] = (byte)((length >> 8) & 0xff);
        buffer[index++] = (byte)(length & 0xff);

        if(length > 0) {
            System.arraycopy(body, 0, buffer, index, length);
        }

        return buffer;
    }

    private static ArrayList<BaseIncomingMessage> decode(byte[] buffer) {
        int offset = 0;
        byte[] bytes = buffer.clone();
        int length = 0;
        ArrayList<BaseIncomingMessage> messages = new ArrayList<>();
        while(offset < bytes.length) {

            int type = bytes[offset++];
            length = ((bytes[offset++]) << 16 | (bytes[offset++]) << 8 | bytes[offset++]);
            byte[] body = length > 0 ? new byte[length] : null;

            System.arraycopy(body, 0, bytes, offset, length);
            offset += length;

            JsonObject obj = (JsonObject) (new JsonParser().parse(new String(bytes)));
            BaseIncomingMessage msg = new BaseIncomingMessage(type, obj);
            messages.add(msg);
        }
        return messages;
    }


    ///////////////////////////////////////////////////////////////////////
    /////////////////////////// MESSAGE HOLDERS ///////////////////////////
    ///////////////////////////////////////////////////////////////////////

    static class IncomingMessage {


        private int type;
        private int id = -1;
        private String route = null;
        private JsonObject data;

        IncomingMessage(int type, int id, JsonObject data) {
            this.type = type;
            this.id = id;
            this.data = data;
        }

        IncomingMessage(int type, String route, JsonObject data) {
            this.type = type;
            this.route = route;
            this.data = data;
        }

        public boolean isIDType() {
            return id >= 0;
        }

        public boolean isRouteType() {
            return route != null;
        }

        public int getType() {
            return type;
        }

        public int getId() {
            return id;
        }

        public String getRoute() {
            return route;
        }

        public JsonObject getObject() {
            return data;
        }
    }

    static class BaseIncomingMessage {


        private int type;
        private JsonObject body;

        public BaseIncomingMessage(int type, JsonObject body) {
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

    public interface MessageListener {
        void call(JsonObject obj);
    }

    /*******************
     *  CALLBACKS: (apart from routing)
     *  - onHandshake
     *  - onHeartbeatReceived
     *  - onClose
     *  - onKick
     *  - onFatalError
     *  -
     *
     */

}
