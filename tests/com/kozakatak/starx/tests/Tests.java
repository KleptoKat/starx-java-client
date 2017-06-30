package com.kozakatak.starx.tests;

import com.google.gson.JsonObject;
import com.kozakatak.starx.Session;

import java.util.Timer;
import java.util.TimerTask;

/**
 * Created by Kozak on 2017-06-26.
 */
public class Tests {

    static class JoinGame implements Session.MessageListener {
        @Override
        public void call(JsonObject obj) {

        }
    }




    public static void itConnectsToTheServerAndRegisters() {
        final Session s = new Session("localhost", 3250);
        s.connect();
        s.start();
        s.on("JoinGame", new JoinGame());

        s.setHeartbeatCallback(new Session.CallbackListener() {
            @Override
            public void call() {
                System.out.println("HEARTBEAT RECEIVED.");
            }
        });

        s.setHandshakeCallback(new Session.CallbackListener() {
            @Override
            public void call() {
                System.out.println("handshake!");
                JsonObject obj = new JsonObject();
                obj.addProperty("name", "bill");

                s.request("Manager.Register", obj, new Session.MessageListener() {
                    @Override
                    public void call(JsonObject obj) {
                        System.out.println(obj);
                        final int id = obj.get("data").getAsJsonObject().get("id").getAsNumber().intValue();
                        String key = obj.get("data").getAsJsonObject().get("key").getAsString();

                        JsonObject auth = new JsonObject();
                        auth.addProperty("id", id);
                        auth.addProperty("key", key);


                        s.request("Manager.Authenticate", auth, new Session.MessageListener() {
                            @Override
                            public void call(JsonObject obj) {
                                System.out.println(obj);
                                JsonObject acc = new JsonObject();
                                acc.addProperty("account_id", id);
                                s.request("Manager.RetrieveAccountInfo", acc, new Session.MessageListener() {
                                    @Override
                                    public void call(JsonObject obj) {
                                        System.out.println(obj);



                                    }
                                });

                            }
                        });
                    }
                });
            }
        });

        s.setConnectFailedCallback(new Session.CallbackListener() {
            @Override
            public void call() {
                System.out.println("Could not connect.");
            }
        });


    }




}
