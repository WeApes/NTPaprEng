package com.weapes.ntpaprseng.crawler.util;

import com.alibaba.fastjson.JSONObject;
import com.weapes.ntpaprseng.crawler.follow.PaperMetricsLink;
import com.weapes.ntpaprseng.crawler.store.DataSource;
import com.zaxxer.hikari.HikariDataSource;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created by 不一样的天空 on 2017/2/4.
 */
public class IndexHelper {
    private  int pageViews;
    private  int webOfScience;
    private  int crossRef;
    private  int scopus;
    private  int newsOutlets;
    private  int reddit;
    private  int blog;
    private  int tweets;
    private  int facebook;
    private  int google;
    private  int pinterest;
    private  int wikipedia;
    private  int mendeley;
    private  int citeUlink;
    private  int zotero;
    private  int f1000;
    private  int video;
    private  int linkedin;
    private  int q_a;
   public static IndexHelper build(){
       return  new IndexHelper();
   }
    public IndexHelper addPageViews(int pageViews) {
        this.pageViews = pageViews;
        return this;
    }

    public IndexHelper addWebOfScience(int webOfScience) {
        this.webOfScience = webOfScience;
        return this;
    }

    public IndexHelper addCrossRef(int crossRef) {
        this.crossRef = crossRef;
        return this;
    }

    public IndexHelper addNewsOutlets(int newsOutlets) {
        this.newsOutlets = newsOutlets;
        return this;
    }

    public IndexHelper addScopus(int scopus) {
        this.scopus = scopus;
        return this;
    }

    public IndexHelper addReddit(int reddit) {
        this.reddit = reddit;
        return this;
    }

    public IndexHelper addTweets(int tweets) {
        this.tweets = tweets;
        return this;
    }

    public IndexHelper addBlog(int blog) {
        this.blog = blog;
        return this;
    }

    public IndexHelper addFacebook(int facebook) {
        this.facebook = facebook;
        return this;
    }

    public IndexHelper addGoogle(int google) {
        this.google = google;
        return this;
    }

    public IndexHelper addPinterest(int pinterest) {
        this.pinterest = pinterest;
        return this;
    }

    public IndexHelper addWikipedia(int wikipedia) {
        this.wikipedia = wikipedia;
        return this;
    }

    public IndexHelper addMendeley(int mendeley) {
        this.mendeley = mendeley;
        return this;
    }

    public IndexHelper addCiteUlink(int citeUlink) {
        this.citeUlink = citeUlink;
        return this;
    }

    public IndexHelper addZotero(int zotero) {
        this.zotero = zotero;
        return this;
    }

    public IndexHelper addF1000(int f1000) {
        this.f1000 = f1000;
        return this;
    }

    public IndexHelper addVideo(int video) {
        this.video = video;
        return this;
    }

    public IndexHelper addLinkedin(int linkedin) {
        this.linkedin = linkedin;
        return this;
    }

    public IndexHelper addQ_a(int q_a) {
        this.q_a = q_a;
        return this;
    }
    public  double getFinalIndex()  throws Exception{
        JSONObject jsonObject=Helper.fileMapToJSONObject("conf"+ File.separator+"weight.json").getJSONObject("weight");
        double finalIndex=jsonObject.getDouble("pageViews")*pageViews;
        finalIndex+=jsonObject.getDouble("webOfScience")*webOfScience;
        finalIndex+=jsonObject.getDouble("crossRef")*crossRef;
        finalIndex+=jsonObject.getDouble("scopus")*scopus;
        finalIndex+=jsonObject.getDouble("newsOutlets")*newsOutlets;
        finalIndex+=jsonObject.getDouble("reddit")*reddit;
        finalIndex+=jsonObject.getDouble("blog")*blog;
        finalIndex+=jsonObject.getDouble("tweets")*tweets;
        finalIndex+=jsonObject.getDouble("facebook")*facebook;
        finalIndex+=jsonObject.getDouble("google")*google;
        finalIndex+=jsonObject.getDouble("pinterest")*pinterest;
        finalIndex+=jsonObject.getDouble("wikipedia")*wikipedia;
        finalIndex+=jsonObject.getDouble("mendeley")*mendeley;
        finalIndex+=jsonObject.getDouble("citeUlink")*citeUlink;
        finalIndex+=jsonObject.getDouble("zotero")*zotero;
        finalIndex+=jsonObject.getDouble("f1000")*f1000;
        finalIndex+=jsonObject.getDouble("video")*video;
        finalIndex+=jsonObject.getDouble("linkedin")*linkedin;
        finalIndex+=jsonObject.getDouble("q_a")*q_a;
        return finalIndex;
    }

    public static void updateFinalIndex(){
        final HikariDataSource mysqlDataSource = DataSource.getMysqlDataSource();
        //从第二张数据表中取出已有所有论文相关指标页面链接和各项指标的数值
        //然后通过最新的权重来重新计算最终的指标，通过URL和时间来更新
        try (final Connection connection = mysqlDataSource.getConnection()) {
            try (final PreparedStatement preparedStatement = connection.prepareStatement("SELECT * FROM REF_DATA")) {
                try (ResultSet results = preparedStatement.executeQuery()) {
                    while (results.next()) {
                        final String url = results.getString("URL");
                        final String updateTime = results.getString("UpdateTime");
                        Double finalIndex=0.0;
                        try {
                            finalIndex= IndexHelper.build().addPageViews(results.getInt("PageViews"))
                                    .addWebOfScience(results.getInt("WebOfScience"))
                                    .addCrossRef(results.getInt("crossRef"))
                                    .addScopus(results.getInt("Scopus"))
                                    .addNewsOutlets(results.getInt("NewsOutlets"))
                                    .addReddit(results.getInt("Reddit"))
                                    .addBlog(results.getInt("Blog"))
                                    .addTweets(results.getInt("Tweets"))
                                    .addFacebook(results.getInt("Facebook"))
                                    .addGoogle(results.getInt("Google"))
                                    .addPinterest(results.getInt("Pinterest"))
                                    .addWikipedia(results.getInt("Wikipedia"))
                                    .addMendeley(results.getInt("Mendeley"))
                                    .addCiteUlink(results.getInt("CiteUlink"))
                                    .addZotero(results.getInt("Zotero"))
                                    .addF1000(results.getInt("F1000"))
                                    .addVideo(results.getInt("Video"))
                                    .addLinkedin(results.getInt("Linkedin"))
                                    .addQ_a(results.getInt("Q_A"))
                                    .getFinalIndex();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        //通过URL和时间来唯一确定要更新的一条记录的最终指标
                        changeFinalIndex(url,updateTime,finalIndex);
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("Connected DB Failed:"+e.getMessage());
        }
    }
   private static void changeFinalIndex(String url,String updateTime,Double finalIndex){
       final HikariDataSource mysqlDataSource = DataSource.getMysqlDataSource();
       //构造更新最终指标的SQL语句
       final String UPDATE_FINAL_INDEX_SQL="UPDATE REF_DATA SET FinalIndex="+finalIndex
               +" WHERE URL="+"\""+url+"\""+" AND UpdateTime="+"\""+updateTime+"\"";
       try (final Connection connection = mysqlDataSource.getConnection()) {
           try (final PreparedStatement preparedStatement = connection.prepareStatement(UPDATE_FINAL_INDEX_SQL)) {
               boolean success=preparedStatement.executeUpdate()!=0;
               if (!success){
                   System.out.println("更新最终指标失败");
               }
           }
       } catch (SQLException e) {
           System.out.println("Connected DB Failed"+e.getMessage());
       }
   }
}
