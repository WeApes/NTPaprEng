package com.weapes.ntpaprseng.crawler.extract;

import com.weapes.ntpaprseng.crawler.follow.AdvSearchedLink;
import com.weapes.ntpaprseng.crawler.follow.Link;
import com.weapes.ntpaprseng.crawler.follow.PaperLink;
import com.weapes.ntpaprseng.crawler.log.Log;
import com.weapes.ntpaprseng.crawler.store.DataSource;
import com.weapes.ntpaprseng.crawler.util.Helper;
import com.zaxxer.hikari.HikariDataSource;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by lawrence on 16/8/8.
 */
public class AdvSearchedWebPage extends WebPage {

    private static final String LAST_URL_UPDATE_SQL =
            "UPDATE HELPER SET "+" last_url = ? "+ "WHERE id = 1";

    // 每页论文数量
    private static final int NUM_OF_PAPERS_PER_PAGE = 25;

    // 抽取多个论文链接的CSS选择器
    private static final String PAPER_LINK_CSS_SELECTOR =
            "h2.h3.extra-tight-line-height a";

    // 抽取网页论文总数的CSS选择器
    private static final String PAPERS_TOTAL_NUM_SELECTOR =
            "p.text13.tiny-space-below.mb0.pt4 > span:last-child";

    public AdvSearchedWebPage(final String text, final String url) {
        super(text, url, true);
    }

    @Override
    public ExtractedObject extract() {
        return null;
    }

    @Override
    public List<? extends Link> extractAll() {
        System.out.println("链接解析: url=" + getUrl() + " type=AdvSearched");
        final Document dom = Jsoup.parse(getText());
        // 所有链接集合
        final List<Link> allLinks = new ArrayList<>();

        if (isFirstPage()) {
            allLinks.addAll(getSiblingLinks(dom));
            Helper.advSearchLinkNum = allLinks.size() + 1; //获得其他页面个数
        }

        // 得到目前页面论文链接
        final List<? extends Link> paperLinks =
                getPaperLinks(parsePaperLinks(dom));
        allLinks.addAll(paperLinks);

        Helper.advSearchLinkNum--; //高级检索后的页面数递减
        return allLinks;
    }

    private final boolean isPaperLinksToBeCrawled(final String url) {
        // 保存第一篇论文链接，即本次爬取最新论文链接到HELPER的last_url字段
        saveFirstUrl(url);

        // 利用上次爬取的最后一篇论文链接来遍历对比本次爬取一定范围内的所有链接，
        // 来确定本次需要爬取得论文链接，当匹配到上次爬取最后一篇论文链接则说明后面的就是上次已经爬取过的，即检查完毕
        String lastUrlForLastTime = Helper.lastUrlForLastTime;
        if (lastUrlForLastTime == null || lastUrlForLastTime.equals("")){
            Log.getUrlNumbers().addAndGet(1);
            return true;
        }

        if (url.equals(lastUrlForLastTime)) {// 匹配到
            Helper.isQueryFinished = true; //检查完毕
            return false; //上次已经爬过，不用再爬
        } else if (!Helper.isQueryFinished) { // 未匹配到且没有检查完毕，则说明是本次待爬取得论文
            Log.getUrlNumbers().addAndGet(1);
            return true;
        }
        //没匹配到且检查完毕，说明是上次爬取过的非最后一篇论文，本次不再爬
        return false;
    }

    private static void saveFirstUrl(String url) {
        if (Helper.isFirstUrl) {//如果是第一篇论文，则更新论文链接到表HELPER中的last_url字段
            boolean isSucceed = false;
            final HikariDataSource mysqlDataSource = DataSource.getMysqlDataSource();
            try (final Connection connection = mysqlDataSource.getConnection()){
                try (final PreparedStatement preparedStatement = connection.prepareStatement(LAST_URL_UPDATE_SQL)) {
                    preparedStatement.setString(1, url);
                    isSucceed = preparedStatement.executeUpdate() != 0;
                }catch (SQLException e){
                    e.printStackTrace();
                }
            }catch (SQLException e){
                e.printStackTrace();
            }
            if (isSucceed) {//成功之后就更改isFirstUrl为false 即后面的论文都不是第一篇了
                Helper.isFirstUrl = false;
            }
        }
    }

    // 得到其他AdvSearched页面链接
    private List<Link> getSiblingLinks(final Document dom) {
        List<Link> siblingLinks = new ArrayList<>();
        for (int i = 2; i <= parsePageNum(dom); i++) {
            siblingLinks.add(new AdvSearchedLink(buildURLWithPageOrder(i)));
        }
        return siblingLinks;
    }

    // 得到当前AdvSearched页面论文链接
    private List<? extends Link> getPaperLinks(final Elements paperLinks) {
        return paperLinks.stream()
                .map(link -> new PaperLink(link.attr("href")))
                .filter(paper -> Helper.isURL(paper.getUrl())).filter(paper -> isPaperLinksToBeCrawled(paper.getUrl()))
                .collect(Collectors.toList());
    }

    // 通过URL判断是否是第一个AdvSearched
    private boolean isFirstPage() {
        return !getUrl().contains("page");
    }

    // 得到AdvSearched链接总页数
    private int parsePageNum(final Document dom) {
        final int totalNum =
                Integer.parseInt(parseTotalNumSpan(dom).text().trim());

        return (totalNum % NUM_OF_PAPERS_PER_PAGE) == 0
                ? totalNum / NUM_OF_PAPERS_PER_PAGE
                : (totalNum / NUM_OF_PAPERS_PER_PAGE) + 1;
    }

    // 构建其他AdvSearched链接。
    private String buildURLWithPageOrder(final int index) {
        return getUrl() + "&page=" + index;
    }

    private Elements parsePaperLinks(final Document dom) {
        return dom.select(PAPER_LINK_CSS_SELECTOR);
    }

    private Elements parseTotalNumSpan(Document dom) {
        return dom.select(PAPERS_TOTAL_NUM_SELECTOR);
    }
}
