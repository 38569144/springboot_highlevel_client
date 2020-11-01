package warframe.limbo.elasticsearch;

import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import javax.annotation.Resource;
import java.io.IOException;

@RunWith(SpringRunner.class)
@SpringBootTest
public class RequestBodyTest {
    @Resource
    private RestHighLevelClient client;
    //初始化数据
    @Test
    public void initData() throws IOException {
        //批量新增操作
        BulkRequest request = new BulkRequest();
        request.add(new IndexRequest().type("_doc").index("shopping01").source(XContentType.JSON, "title", "小米手机", "images", "http://www.gulixueyuan.com/xm.jpg", "price", 1999.0));
        request.add(new IndexRequest().type("_doc").index("shopping01").source(XContentType.JSON, "title", "小米电视", "images", "http://www.gulixueyuan.com/xmds.jpg", "price", 2999.0));
        request.add(new IndexRequest().type("_doc").index("shopping01").source(XContentType.JSON, "title", "华为手机", "images", "http://www.gulixueyuan.com/hw.jpg", "price", 4999.0, "subtitle", "小米"));
        request.add(new IndexRequest().type("_doc").index("shopping01").source(XContentType.JSON, "title", "apple手机", "images", "http://www.gulixueyuan.com/appletl.jpg", "price", 5999.00));
        request.add(new IndexRequest().type("_doc").index("shopping01").source(XContentType.JSON, "title", "apple", "images", "http://www.gulixueyuan.com/apple.jpg", "price", 3999.00));
        BulkResponse response = client.bulk(request, RequestOptions.DEFAULT);
        System.out.println("took::" + response.getTook());
        System.out.println("Items::" + response.getItems());
    }
    //请求体查询_基本查询
    @Test
    public void basicQuery() throws IOException {
        //1. 创建请求对象
        SearchRequest request = new SearchRequest().indices("shopping01").types("_doc");
        //构建查询的请求体
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        //查询所有
        //sourceBuilder.query(QueryBuilders.matchAllQuery());
        //match查询,带分词器的查询
        //sourceBuilder.query(QueryBuilders.matchQuery("title", "小米手机").operator(Operator.AND));
        //term查询,不带分词器,查询条件作为关键词
        sourceBuilder.query(QueryBuilders.termQuery("price", 1999));
        request.source(sourceBuilder);
        //2. 客户端发送请求,获取响应对象
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        //3. 打印输出结果
        printResult(response);
        //4. 关闭连接
        client.close();
    }
    //打印输出结果
    private void printResult(SearchResponse response) {
        SearchHits hits = response.getHits();
        System.out.println("took:" + response.getTook());
        System.out.println("timeout:" + response.isTimedOut());
        System.out.println("total:" + hits.getTotalHits());
        System.out.println("MaxScore:" + hits.getMaxScore());
        System.out.println("hits========>>");
        for (SearchHit hit : hits) {
            //输出每条查询的结果信息
            System.out.println("id:"+hit.getId());
            System.out.println("source:"+hit.getSourceAsString());
        }
        System.out.println("<<========");
    }
    //查询的字段过滤,分页,排序
    @Test
    public void fetchSourceAndSortAndByPage() throws IOException {
        //1.创建请求对象
        SearchRequest request = new SearchRequest().indices("shopping01").types("_doc");
        //构建查询的请求体
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        //查询所有
        sourceBuilder.query(QueryBuilders.matchAllQuery());
        //分页信息
        //当前页其实索引(第一条数据的顺序号)，from
        sourceBuilder.from(2);
        //每页显示多少条size
        sourceBuilder.size(2);
        //排序信息,参数一：排序的字段，参数二：顺序ASC升序，降序DESC
        sourceBuilder.sort("price", SortOrder.ASC);
        //查询字段过滤
        String[] excludes = {};
        String[] includes = {"title", "subtitle", "price"};
        sourceBuilder.fetchSource(includes, excludes);
        request.source(sourceBuilder);
        //2.客户端发送请求，获取响应对象
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        //3.打印结果信息
        printResult(response);
    }
    /**
     * 高级查询
     */
    @Test
    public void boolAndRangeAndFuzzyQuery() throws IOException {
        //1.创建请求对象
        SearchRequest request = new SearchRequest().indices("shopping01").types("_doc");
        //构建查询的请求体
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        //高级查询的三种方式：
        //-----------------------------------------------------------------------------
        //bool查询：查询title中必须包含小米，一定不含有电视，应该含有手机的所有商品
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        //must
        //boolQueryBuilder.must(QueryBuilders.matchQuery("title", "小米"));
        //must not
        //boolQueryBuilder.mustNot(QueryBuilders.matchQuery("title", "电视"));
        //should
        //.should(QueryBuilders.matchQuery("title", "手机"));
        //sourceBuilder.query(boolQueryBuilder);
        //-----------------------------------------------------------------------------
        //范围查询：查询价格大于3千，小于5千的所有商品
        //RangeQueryBuilder rangeQuery = QueryBuilders.rangeQuery("price");
        //#### gt 大于(greater than)
        //rangeQuery.gt("3000");
        //#### lt 小于(less than)
        //rangeQuery.lt("5000");
        //#### gte 大于等于(greater than equals)
        //#### lte 小于等于(less than equals)
        //sourceBuilder.query(rangeQuery);
        //-----------------------------------------------------------------------------
        //模糊查询：查询包含apple关键词的所有商品，完成模糊查询cpple
        //sourceBuilder.query(QueryBuilders.fuzzyQuery("title","cpple").fuzziness(Fuzziness.ONE));
        //-----------------------------------------------------------------------------
        request.source(sourceBuilder);
        //2.客户端发送请求，获取响应对象
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        //3.打印结果信息
        printResult(response);
    }
}
