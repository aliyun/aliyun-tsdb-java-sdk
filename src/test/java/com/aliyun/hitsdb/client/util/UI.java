package com.aliyun.hitsdb.client.util;

import java.io.IOException;

public class UI {
    public static void pauseStart() {
        System.out.println("按下任意键，开始运行...");
        while (true) {
            int read;
            try {
                read = System.in.read();
                if (read != 0) {
                    break;
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        System.out.println("开始运行");
    }
}
