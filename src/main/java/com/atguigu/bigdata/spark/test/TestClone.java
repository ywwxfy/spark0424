package com.atguigu.bigdata.spark.test;

import java.util.ArrayList;

public class TestClone {
    public static void main(String[] args) {

        ArrayList<User> list = new ArrayList<User>();

        User user = new User();
        user.name = "zhangsan";

        list.add(user);

        // 克隆浅复制
        ArrayList<User> list1 = (ArrayList<User>)list.clone();

        //System.out.println(list == list1);
        User user1 = list1.get(0);
        user1.name = "lisi";

        System.out.println(user.name);
        System.out.println(user1.name);

    }
}

class User {
    public String name;
}