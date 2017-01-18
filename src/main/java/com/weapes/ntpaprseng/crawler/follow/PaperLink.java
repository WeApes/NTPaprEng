package com.weapes.ntpaprseng.crawler.follow;

import com.weapes.ntpaprseng.crawler.extract.Extractable;
import com.weapes.ntpaprseng.crawler.extract.PaperWebPage;
import com.weapes.ntpaprseng.crawler.util.Helper;

import java.io.IOException;

/**
 * 论文链接
 * Created by lawrence on 16/8/8.
 */
public class PaperLink extends Link {

    public PaperLink(final String url) {
        super(url);
    }

    @Override
    public Extractable follow() throws IOException {
        while (Helper.advSearchLinkNum > 0) {//如果高级检索链接没有follow完成，论文链接不开始follow
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        // 根据论文链接发起请求 获取论文信息页面
        final String paperWebPage = Helper.fetchWebPage(getUrl());
        return new PaperWebPage(paperWebPage, getUrl());
    }
}
