package com.atguigu.bigdata.spark.test;

import java.util.Random;

public class TestRandom {
    public static void main(String[] args) {

        Random r = new Random(2);
        for ( int i = 1; i <= 5; i++ ) {
            System.out.println(r.nextInt(10));
        }
        System.out.println("****************");
        Random r1 = new Random(2);
        for ( int i = 1; i <= 5; i++ ) {
            System.out.println(r1.nextInt(10));
        }
    }
}
