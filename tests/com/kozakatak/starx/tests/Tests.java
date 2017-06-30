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

    static class OnSearchStateMessage implements Session.MessageListener {
        @Override
        public void call(JsonObject obj) {

        }
    }

    static class OnGameStateMessage implements Session.MessageListener {
        @Override
        public void call(JsonObject obj) {

        }
    }

    static class OnKick implements Session.MessageListener {
        @Override
        public void call(JsonObject obj) {

        }
    }




    public static void itConnectsToTheServer() {
        final Session s = new Session("localhost", 3250);
        s.connect();

        s.on("JoinGame", new JoinGame());
        s.on("SearchStateMessage", new OnSearchStateMessage());
        s.on("GameStateMessage", new OnGameStateMessage());
        s.on("onKick", new OnKick());


        final JsonObject login = new JsonObject();
        login.addProperty("name", "Bill");
        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                s.request("Manager.Register", login, new Session.MessageListener() {
                    @Override
                    public void call(JsonObject obj) {
                        System.out.println("wow");
                    }
                });
                s.request("Manager.Authenticate", login, new Session.MessageListener() {
                    @Override
                    public void call(JsonObject obj) {
                        System.out.println(obj.get("code").getAsInt());
                    }
                });
            }
        }, 800);




    }




}
