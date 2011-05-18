package com.nearinfinity.hadoop.billionstars;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class MakeStars {

    private static final int NUM_STARS = 10000000;
    private static final char CTRL_A = '\u0001';
    private static final int NUM_CHARS = 10;
    private static final int NUM_DIGITS = 12;

    private static Random distRandom = new Random(System.currentTimeMillis());
    private static Random nameRandom = new Random(System.currentTimeMillis());

    public static void main(String[] args) throws IOException {
        long start = System.currentTimeMillis();
        File outFile = new File("stars_data.txt");
        BufferedWriter writer = null;
        try {
            writer = new BufferedWriter(new FileWriter(outFile));
            for (int i = 0; i < NUM_STARS; i++) {
                if (i % 1000000 == 0) System.out.println(i);
                writer.write(createName() + CTRL_A + createDistance() + "\n");
            }
        }
        catch (IOException ioex) {
            System.err.println("Ooops: " + ioex);
        }
        finally {
            if (writer != null) writer.close();
        }
        long end = System.currentTimeMillis();
        long elapsed = end - start;
        System.out.println("Elapsed time: " + (elapsed / 1000.0) + " sec");
    }

    public static String createName() {
        StringBuilder builder = new StringBuilder();
        char[] name = new char[NUM_CHARS];
        for (int i = 0; i < NUM_CHARS; i++) {
            builder.append(new Character((char) (nameRandom.nextInt(26) + 65)).charValue());
        }
        for (int i = 0; i < NUM_DIGITS; i++) {
            builder.append(nameRandom.nextInt(10));
        }
        return builder.toString();
    }

    public static long createDistance() {
        return Math.abs(distRandom.nextLong());
    }

}
