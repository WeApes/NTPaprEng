package com.weapes.ntpaprseng.crawler.crawler;

import com.weapes.ntpaprseng.crawler.util.Helper;
import java.util.concurrent.TimeUnit;

/**
 * Created by Evan Hung on 2017/1/14.
 */
public class Launcher {

    public static void main(String args[]) throws InterruptedException {
        System.out.print("系统运行。\n");
        // 串行执行爬取、更新，每次任务完成任务读取间隔时间并暂停任务，无限循环
        while (true) {
            //爬取
            new PaperCrawler().crawl();
            while (!Helper.isCrawlFinished) Thread.sleep(2000);
            //更新
            new DetailCrawler().crawl();
            while (!Helper.isUpdateFinished) Thread.sleep(2000);

            System.out.print("本次任务完成。\n");
            //爬取、更新状态重置 为下周期做准备
            Helper.isCrawlFinished = false;
            Helper.isUpdateFinished =false;
            //周期间歇
            TimeUnit.SECONDS.sleep(Helper.getTaskPeriod());
        }
    }
}

