package com.kozakatak.starx.tests;

import com.kozakatak.Session;

/**
 * Created by Kozak on 2017-06-26.
 */
public class Tests {



    public static void itConnectsToTheServer() {
        Session s = new Session("localhost", 3250);
        s.connect();


    }




}
