package com.atguigu.bigdata.spark.test;

public class TestClassLoader {
    public static void main(String[] args) {

        // 从classpath中查找文件（配置文件，XML文件）,类路径指的是target/classes,main下面是我们的源码目录
//        System.out.println(Thread.currentThread().getContextClassLoader().getResourceAsStream("/"));
        System.out.println(Thread.currentThread().getContextClassLoader().getResource("word.txt"));
    }
}
