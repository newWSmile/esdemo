package com.smile.esdemo;

import com.google.gson.JsonObject;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Smile(wangyajun)
 * @create 2018-09-07 19:44
 **/
public class ElasticsearchTest {

    private Logger logger = LoggerFactory.getLogger(ElasticsearchTest.class);

    public final static String HOST = "localhost";

    public final static int PORT = 9300;//http请求的端口是9200，客户端是9300

    private TransportClient client = null;


    /**
     * 获取客户端连接信息
     *
     * @return void
     * @throws UnknownHostException
     * @Title: getConnect
     * @author sunt
     * @date 2017年11月23日
     */
    @Before
    public void getConnect() throws UnknownHostException {
        client = new PreBuiltTransportClient(Settings.EMPTY).addTransportAddresses(
                new InetSocketTransportAddress(InetAddress.getByName(HOST), PORT));
        logger.info("连接信息:" + client.toString());
    }


    /**
     * 关闭连接
     *
     * @return void
     * @Title: closeConnect
     * @author sunt
     * @date 2017年11月23日
     */
    @After
    public void closeConnect() {
        if (null != client) {
            logger.info("执行关闭连接操作...");
            client.close();
        }
    }


    /**
     * 创建索引库
     *
     * @return void
     * 需求:创建一个索引库为：msg消息队列,类型为：tweet,id为1
     * 索引库的名称必须为小写
     * @throws IOException
     * @Title: addIndex1
     * @author sunt
     * @date 2017年11月23日
     */
    @Test
    public void addIndex1() throws IOException {
        IndexResponse response = client.prepareIndex("msg", "tweet", "2").setSource(XContentFactory.jsonBuilder()
                .startObject().field("userName", "张三222")
                .field("sendDate", new Date())
                .field("msg", "你好李四222")
                .endObject()).get();

        logger.info("索引名称:" + response.getIndex() + "\n类型:" + response.getType()
                + "\n文档ID:" + response.getId() + "\n当前实例状态:" + response.status());
    }


    @Test
    public void addIndex2() {
        String jsonStr = "{" +
                "\"userName\":\"张三\"," +
                "\"sendDate\":\"2017-11-30\"," +
                "\"msg\":\"你好李四\"" +
                "}";

        IndexResponse response = client.prepareIndex("weixin", "tweet").setSource(jsonStr, XContentType.JSON).get();
        logger.info("json索引名称:" + response.getIndex() + "\njson类型:" + response.getType()
                + "\njson文档ID:" + response.getId() + "\n当前实例json状态:" + response.status());

    }

    @Test
    public void addIndex3() {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("userName", "张三");
        map.put("sendDate", new Date());
        map.put("msg", "你好李四");

        IndexResponse response = client.prepareIndex("momo", "tweet").setSource(map).get();

        logger.info("map索引名称:" + response.getIndex() + "\n map类型:" + response.getType() + "\n map文档ID:"
                + response.getId() + "\n当前实例map状态:" + response.status());
    }


    /**
     * 传递json对象
     * 需要添加依赖:gson
     *
     * @return void
     * @Title: addIndex4
     * @author sunt
     * @date 2017年11月23日
     */
    @Test
    public void addIndex4() {
        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("userName", "张三333");
        jsonObject.addProperty("sendDate", "2017-11-23");
        jsonObject.addProperty("msg", "你好李四333");

        IndexResponse response = client.prepareIndex("qq", "tweet").setSource(jsonObject, XContentType.JSON).get();

        logger.info("jsonObject索引名称:" + response.getIndex() + "\n jsonObject类型:" + response.getType()
                + "\n jsonObject文档ID:" + response.getId() + "\n当前实例jsonObject状态:" + response.status());
    }


    @Test
    public void getData1() {
        GetResponse getResponse = client.prepareGet("msg", "tweet", "1").get();
        logger.info("索引库的数据:" + getResponse.getSourceAsString());
    }


    /**
     * 更新索引库数据
     *
     * @return void
     * @Title: updateData
     * @author sunt
     * @date 2017年11月23日
     */
    @Test
    public void updateData() {
        JsonObject jsonObject = new JsonObject();

        jsonObject.addProperty("userName", "王五");
        jsonObject.addProperty("sendDate", "2008-08-08");
        jsonObject.addProperty("msg", "你好,张三，好久不见");

        UpdateResponse updateResponse = client.prepareUpdate("msg", "tweet", "1")
                .setDoc(jsonObject.toString(), XContentType.JSON).get();

        logger.info("updateResponse索引名称:" + updateResponse.getIndex() + "\n updateResponse类型:" + updateResponse.getType()
                + "\n updateResponse文档ID:" + updateResponse.getId() + "\n当前实例updateResponse状态:" + updateResponse.status());
    }


    /**
     * 根据索引名称，类别，文档ID 删除索引库的数据
     *
     * @return void
     * @Title: deleteData
     * @author sunt
     * @date 2017年11月23日
     */
    @Test
    public void deleteData() {
        DeleteResponse deleteResponse = client.prepareDelete("msg", "tweet", "1").get();

        logger.info("deleteResponse索引名称:" + deleteResponse.getIndex() + "\n deleteResponse类型:" + deleteResponse.getType()
                + "\n deleteResponse文档ID:" + deleteResponse.getId() + "\n当前实例deleteResponse状态:" + deleteResponse.status());
    }


    /**
     * @描述 全局搜索关键字并高亮显示
     * @返回值 void
     * @创建人 Smile(wangyajun)
     * @创建时间 2018/9/10
     */
    @Test
    public void searchData() {
        QueryBuilder matchQuery = QueryBuilders.matchQuery("about", "rock,climbing");
        HighlightBuilder hiBuilder = new HighlightBuilder();
        hiBuilder.preTags("<h2>");
        hiBuilder.postTags("</h2>");
        hiBuilder.field("about");
        // 搜索数据
        SearchResponse response = client.prepareSearch("megacorp").setQuery(matchQuery).highlighter(hiBuilder).execute().actionGet();
        //获取结果
        SearchHits searchHits = response.getHits();
        logger.info("共搜到:" + searchHits.getTotalHits() + "条结果!");
        for (SearchHit hit : searchHits) {
            logger.info("String方式打印文档搜索内容:");
            logger.info(hit.getSourceAsString());
            logger.info("Map方式打印高亮内容");
            logger.info("" + hit.getHighlightFields());

            logger.info("遍历高亮集合，打印高亮片段:");
            Text[] text = hit.getHighlightFields().get("about").getFragments();
            for (Text str : text) {
                logger.info(str.string());
            }
        }


    }

}
