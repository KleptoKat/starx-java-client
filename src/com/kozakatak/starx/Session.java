package com.kozakatak.starx;

import com.google.gson.*;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.Socket;
import java.util.*;




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

    private static final int MAX_CONNECT_ATTEMPTS = 4;
    private static final int RECONNECT_INTERVAL = 1500;

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
    private boolean doConnect = false;
    private boolean running = true;

    private long connectAt = 0;
    private int connectAttempts = 0;

    private String host;
    private int port;
    private Thread thread;
    private Socket s;

    private OutputStream out;
    private InputStream in;
    private Gson gson;

    private Map<String, MessageListener> routeCallbacks;
    private Map<Integer, MessageListener> requestCallbacks;
    private int lastMessageID = 1;
    private ArrayList<byte[]> outBuffer;

    public Session(String host, int port) {
        this.host = host;
        this.port = port; // 3250
        thread = new Thread(this);
        gson = new GsonBuilder().create();
        heartbeatTimer = new Timer();
        heartbeatTimeoutTimer = new Timer();
        routeCallbacks = new HashMap<String, MessageListener>();
        requestCallbacks = new HashMap<Integer, MessageListener>();
        outBuffer = new ArrayList<byte[]>();

        fatalErrorCallback = new ExceptionListener() {
            @Override
            public void call(Exception e) {
                e.printStackTrace();
            }
        };

        errorCallback = new ExceptionListener() {
            @Override
            public void call(Exception e) {
                e.printStackTrace();
            }
        };
    }

    public void start() {
        thread.start();
    }

    public void connect() {
        doConnect = true;
    }

    private boolean executeConnect() {

        try {
            s = new Socket(this.host, this.port);
            out = s.getOutputStream();
            in = s.getInputStream();
            performHandshake();
            return true;

        } catch (IOException e) {
            resetConnection();
            onFatalError(e);
            return false;
        }
    }


    public boolean isConnected() {
        return connected;
    }

    private void resetConnection()
    {
        boolean wasConnected = connected;

        if (outBuffer == null) {
            outBuffer = new ArrayList<byte[]>();
        }
        outBuffer.clear();
        connected = false;
        if (heartbeatTimer != null) {
            heartbeatTimer.cancel();
        }

        if (heartbeatTimeoutTimer != null) {
            heartbeatTimeoutTimer.cancel();
        }

        if (wasConnected) {
            onDisconnect();
        }
    }

    public void close() {
        resetConnection();
        this.running = false;
        onClose();
    }

    private void doWrite() throws IOException {
        if (outBuffer.size() > 0) {
            // write bytes out
            while (outBuffer.size() > 0) {
                if (outBuffer.get(0) != null) {
                    out.write(outBuffer.get(0));
                    outBuffer.remove(0);
                }
            }
            out.flush();
        }
    }

    private void doRead() throws IOException {
        if (in.available() > 0) {
            // read bytes in
            int type = in.read();
            if (type == -1) {
                resetConnection();
                return;
            }

            int length = in.read() << 16 | in.read() << 8 | in.read();
            byte[] bytes = new byte[length];

            //noinspection ResultOfMethodCallIgnored
            in.read(bytes, 0, length);
            processPackage(type, bytes);
        }
    }

    @Override
    public void run() {

        while (running) {
            try {
                if (!connected && (doConnect || (connectAt > 0 && connectAt < System.currentTimeMillis()))) {
                    connectAt = 0;
                    doConnect = false;
                    connected = this.executeConnect();

                    if (connected) {
                        onConnect();
                        connectAttempts = 0;
                    } else {
                        onConnectFailed();
                        if (connectAttempts < MAX_CONNECT_ATTEMPTS) {
                            connectAt = System.currentTimeMillis() + RECONNECT_INTERVAL;
                        }
                        connectAttempts++;
                    }
                }

                if (connected) {
                    doWrite();
                    doRead();
                }

            } catch (IOException e) {
                onFatalError(e);
                close();
            } finally {
                Thread.yield();
            }
        }


        try {
            s.close();
            in.close();
            out.close();
        } catch (IOException ignored) {}
    }


    public Socket getSocket() {
        return s;
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
            onFatalError(new OldClientException());
            resetConnection();
            return;
        }

        if (code != RES_OK) {
            resetConnection();
            onFatalError(new HandshakeException());
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
        onHandshake();

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
                            onFatalError(new ServerTimeoutException());
                            resetConnection();
                        }
                    }
                }, heartbeatTimeoutInterval);
            }
        }, heartbeatInterval);

        onHeartbeat();

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
            } else {
                onError(new InvalidMessageException());
            }
        }

        if (msg.isRouteType()) {
            MessageListener l = routeCallbacks.get(msg.getRoute());
            if (l != null) {
                l.call(msg.getObject());
                routeCallbacks.remove(msg.getRoute());
            } else {
                onError(new InvalidMessageException());
            }
        }
    }

    private void processKick(byte[] bytes) {

        try {
            JsonElement element = (new JsonParser()).parse(new String(bytes));
            if (element != null && element.isJsonObject()) {
                onKick(element.getAsJsonObject());
            }

        } catch(JsonSyntaxException e) {
            onKick(null);
        } finally {
            resetConnection();
        }
    }

    ///////////////////////////////////////////////////////////////////////
    /////////////////////////////// SENDERS ///////////////////////////////
    ///////////////////////////////////////////////////////////////////////


    public void notify(String route) { notify(route, new JsonObject()); }
    public void notify(String route, JsonObject data) {
        if (!connected) {
            return;
        }

        byte[] msg = encodeMessage(0, TYPE_REQUEST, route, data);
        if (msg != null) {
            send(TYPE_DATA, msg);
        }
    }


    public void request(String route, MessageListener cb) { request(route, new JsonObject(), cb); }
    public void request(String route, JsonObject data, MessageListener cb) {
        if (!connected) {
            return;
        }
        int id = lastMessageID++;

        byte[] msg = encodeMessage(id, TYPE_REQUEST, route, data);
        if (msg != null) {
            if (cb != null) {
                requestCallbacks.put(id, cb);
            }
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

    private void performHandshake() {
        send(TYPE_HANDSHAKE, getHandshake());
    }

    private void send(int type, byte[] bytes) {
        byte[] encodedBytes = encode(type, bytes);
        outBuffer.add(encodedBytes);
    }

    private void send(int type, JsonElement element) {
        send(type, gson.toJson(element).getBytes());
    }

    ///////////////////////////////////////////////////////////////////////
    ////////////////////////////// HANDLERS ///////////////////////////////
    ///////////////////////////////////////////////////////////////////////

    private CallbackListener handshakeCallback;
    private CallbackListener heartbeatCallback;
    private CallbackListener closeCallback;
    private CallbackListener disconnectCallback;
    private MessageListener kickCallback;
    private CallbackListener connectCallback;
    private CallbackListener connectFailedCallback;
    private ExceptionListener fatalErrorCallback;
    private ExceptionListener errorCallback;

    private void onClose() { if (closeCallback != null) closeCallback.call(); }
    private void onHeartbeat() { if (heartbeatCallback != null) heartbeatCallback.call(); }
    private void onHandshake() { if (handshakeCallback != null) handshakeCallback.call(); }
    private void onKick(JsonObject obj) { if (kickCallback != null) kickCallback.call(obj); }
    private void onDisconnect() { if (disconnectCallback != null) disconnectCallback.call(); }
    private void onConnect() { if (connectCallback != null) connectCallback.call(); }
    private void onConnectFailed() { if (connectFailedCallback != null) connectFailedCallback.call(); }
    private void onFatalError(Exception e) { if (fatalErrorCallback != null) fatalErrorCallback.call(e); }
    private void onError(Exception e) { if (errorCallback != null) errorCallback.call(e); }


    public void setHandshakeCallback(CallbackListener handshakeCallback) {
        this.handshakeCallback = handshakeCallback;
    }

    public void setHeartbeatCallback(CallbackListener heartbeatCallback) {
        this.heartbeatCallback = heartbeatCallback;
    }

    public void setCloseCallback(CallbackListener closeCallback) {
        this.closeCallback = closeCallback;
    }

    public void setDisconnectCallback(CallbackListener disconnectCallback) {
        this.disconnectCallback = disconnectCallback;
    }

    public void setKickCallback(MessageListener kickCallback) {
        this.kickCallback = kickCallback;
    }


    public void setConnectCallback(CallbackListener connectCallback) {
        this.connectCallback = connectCallback;
    }

    public void setConnectFailedCallback(CallbackListener connectFailedCallback) {
        this.connectFailedCallback = connectFailedCallback;
    }

    public void setFatalErrorCallback(ExceptionListener fatalErrorCallback) {
        this.fatalErrorCallback = fatalErrorCallback;
    }


    public void setErrorCallback(ExceptionListener errorCallback) {
        this.errorCallback = errorCallback;
    }

    public boolean on(String route, MessageListener l) {

        if (routeCallbacks.get(route) != null) {
            return false;
        }

        routeCallbacks.put(route, l);
        return true;
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
                    onError(new RouteLengthTooLong());
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
        ArrayList<BaseIncomingMessage> messages = new ArrayList<BaseIncomingMessage>();
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

    public interface CallbackListener {
        void call();
    }


    public interface ExceptionListener {
        void call(Exception e);
    }

    public class OldClientException extends HandshakeException {
        private OldClientException() {
            super("Client is outdated.");
        }
    }

    public class ServerTimeoutException extends Exception {}
    public class InvalidMessageException extends Exception {}
    public class RouteLengthTooLong extends Exception {}

    public class HandshakeException extends Exception {
        private HandshakeException() {
            super();
        }

        private HandshakeException(String message) {
            super(message);
        }
    }

}
