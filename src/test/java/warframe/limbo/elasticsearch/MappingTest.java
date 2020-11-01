package warframe.limbo.elasticsearch;

import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetMappingsRequest;
import org.elasticsearch.client.indices.GetMappingsResponse;
import org.elasticsearch.client.indices.PutMappingRequest;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import javax.annotation.Resource;
import java.io.IOException;

@SpringBootTest
@RunWith(SpringRunner.class)
public class MappingTest {
    @Resource
    private RestHighLevelClient client;
    //目标: 配置映射,第一种方式,使用XContentBuilder,构建请求体
    @Test
    public void putMapping01() throws IOException {
        //1. 创建请求对象: 配置映射
        PutMappingRequest request = new PutMappingRequest("shopping01");
        //构建请求体
        XContentBuilder jsonBuilder = XContentFactory.jsonBuilder();
        jsonBuilder.startObject()
                .startObject("properties")
                .startObject("title")
                .field("type","text").field("analyzer","ik_max_word")
                .endObject()
                .startObject("subtitle")
                .field("type","text").field("analyzer","ik_max_word")
                .endObject()
                .startObject("images")
                .field("type","keyword").field("index",false)
                .endObject()
                .startObject("price")
                .field("type","float")
                .endObject()
                .endObject()
                .endObject();
        //设置请求体,source("请求体JSON构建器对象")
        request.source(jsonBuilder);
        //2. 客户端发送请求,获取响应对象
        AcknowledgedResponse response = client.indices().putMapping(request, RequestOptions.DEFAULT);
        //3. 打印响应结果
        System.out.println("acknowledged: "+response.isAcknowledged());
        //4. 关闭客户端,释放连接资源
        client.close();
    }
    //目标: 配置映射,第二种方式,使用JSON字符
    @Test
    public void putMapping02() throws IOException {
        //1. 创建请求对象: 配置映射
        PutMappingRequest request = new PutMappingRequest("shopping02");
        //设置请求体,source("请求体json字符串","请求体数据类型")
        request.source("{\"properties\":{"
                + "\"title\":{\"type\":\"text\",\"analyzer\":\"ik_max_word\"},"
                + "\"subtitle\":{\"type\":\"text\",\"analyzer\":\"ik_max_word\"},"
                + "\"price\":{\"type\":\"float\"},"
                + "\"images\":{\"type\":\"keyword\",\"index\":false}}}",
                XContentType.JSON
        );
        //2. 客户端发送请求,获取响应对象
        AcknowledgedResponse response = client.indices().putMapping(request, RequestOptions.DEFAULT);
        //3. 打印响应结果
        System.out.println("acknowledged:"+response.isAcknowledged());
        //4. 关闭客户端,释放连接
        client.close();
    }
    //查看映射
    @Test
    public void getMapping() throws IOException {
        //1. 创建请求对象: 查看映射
        GetMappingsRequest request = new GetMappingsRequest();
        //设置索引库名称
        request.indices("shopping01");
        //2. 客户端发送请求,获取响应对象
        GetMappingsResponse response = client.indices().getMapping(request, RequestOptions.DEFAULT);
        //3. 打印响应结果
        System.out.println("mappings:"+response.mappings());
        System.out.println("source:"+response.mappings().get("shopping01").getSourceAsMap());
        //4. 关闭客户端,释放连接
        client.close();
    }
}
