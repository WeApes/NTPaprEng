package com.weapes.ntpaprseng.crawler.crawler;
import com.weapes.ntpaprseng.crawler.util.Helper;


import java.util.concurrent.TimeUnit;

/**
 * Created by Evan Hung on 2017/1/14.
 */
public class Launcher {
    public static void main(String args[]) {
        System.out.print("系统运行。\n");
        while (true) {
            new PaperCrawler().crawl();
            while (!Helper.isCrawlFinished()) {
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            new DetailCrawler().crawl();
            while (!Helper.isUpdateFinished()) {
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            try {
                long pause = Helper.getTaskPeriod();
                System.out.print(pause + "!!!!!!!!!!!\n");
                TimeUnit.SECONDS.sleep(pause);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}

