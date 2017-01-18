package com.weapes.ntpaprseng.crawler.crawler;

import com.weapes.ntpaprseng.crawler.extract.Extractable;
import com.weapes.ntpaprseng.crawler.extract.ExtractedObject;
import com.weapes.ntpaprseng.crawler.follow.Followable;
import com.weapes.ntpaprseng.crawler.follow.Link;
import com.weapes.ntpaprseng.crawler.log.Log;
import com.weapes.ntpaprseng.crawler.store.Storable;
import com.weapes.ntpaprseng.crawler.util.Helper;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static com.weapes.ntpaprseng.crawler.log.Log.getCrawlingFailedNumber;
import static com.weapes.ntpaprseng.crawler.log.Log.getCrawlingSucceedNumbers;


/**
 * Created by lawrence on 16/8/8.
 */
class StorableFetcher<F extends Followable> implements Runnable {

    private final ExecutorService creator;
    private final ExecutorService consumer;
    private final F seed;

    StorableFetcher(final ExecutorService creator,
                    final ExecutorService consumer,
                    final F seed) {
        this.creator = creator;
        this.consumer = consumer;
        this.seed = seed;
    }

    /**
     * 爬虫运行框架,如果链接follow后得到Extractable。
     * 根据其能抽取成一个或多个followable/storable来做再分发。
     */
    @Override
    public void run() {
        try {
            // 对高级检索后链接或论文链接、论文指标链接follow得到页面
            final Extractable extractable = seed.follow();
            if (!extractable.isMulti()) { //如果是论文信息页面或论文指标页面
                dispatch(extractable.extract());
            } else {//如果是高级检索后论文入口页面
                List<? extends ExtractedObject> extractedObjectsList = extractable.extractAll();
                if (extractedObjectsList.size() == 0){//没有新刊登的待爬取论文
                    Helper.isFirstUrl = true; //下次任务开始时有第一条论文url
                    Helper.isCrawlFinished = true; //爬取任务结束
                    getCrawlingSucceedNumbers().set(0);
                    getCrawlingFailedNumber().set(0);
                    Log.getCrawlingNumbers().set(0);

                    System.out.println("本次爬取论文" + Log.getUrlNumbers() + "篇。");
                    System.out.println("爬取结束。");
                }

                extractedObjectsList.forEach(this::dispatch);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * 分别根据参数的类型做分发。
     * 如果是Followable则继续交给生产者,
     * 如果是Storable交给消费者分发。
     *
     * @param extractedObject 被抽取后的Obj,可以为链接或可存储的对象
     */
    private void dispatch(final ExtractedObject extractedObject) {
        if (extractedObject instanceof Followable) {//如果是链接，则继续生产，进行follow和extract
             creator.submit(new StorableFetcher<>(creator, consumer, (Link) extractedObject));
        } else {//如果不是链接 就是storable的爬取数据，进行消费
            consumer.submit(new StorableHandler<>((Storable) extractedObject));
        }
    }

}
