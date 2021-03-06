package com.techienotes;

import com.techienotes.exceptions.ValidationException;

import java.io.IOException;

public class App {

    public static void main(String[] args) {

        SparkJob sparkJob = new SparkJob(args);
        try {
            sparkJob.startJob();
        } catch (ValidationException | IOException e) {
            e.printStackTrace();
        }
    }
}
