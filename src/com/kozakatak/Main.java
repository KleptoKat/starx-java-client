package com.kozakatak;

import com.kozakatak.starx.tests.Tests;

import java.io.IOException;
import java.util.Scanner;

public class Main {

    public static void main(String[] args) {
	// write your code here
        Tests.itConnectsToTheServer();

        Scanner scanner = new Scanner(System.in);
        scanner.nextLine();
    }
}
