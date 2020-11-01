package warframe.limbo.elasticsearch;

import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import javax.annotation.Resource;
import java.io.IOException;

@SpringBootTest
@RunWith(SpringRunner.class)
public class IndexTest {

    @Resource
    private RestHighLevelClient restHighLevelClient;

    /**
     * 目标: 创建索引库
     * 1. 创建请求对象,设置索引库名称
     * 2. 客户端发送请求,获取响应对象
     * 3. 打印响应对象中的返回结果
     * 4. 关闭客户端,释放连接资源
     */
    @Test
    public void create() throws IOException {
        //1.创建请求对象,创建索引的请求
        CreateIndexRequest request = new CreateIndexRequest("shopping02");
        //2.客户端发送请求,获取响应对象
        CreateIndexResponse response = restHighLevelClient.indices().create(request, RequestOptions.DEFAULT);
        //3.打印响应对象中的返回结果
        //返回index信息
        System.out.println("index: "+response.index());
        //acknowledged代表创建成功
        System.out.println("acknowledged:"+response.isAcknowledged());
        //4.关闭客户端,释放连接资源
        restHighLevelClient.close();
    }
    //查询索引库
    @Test
    public void getIndex() throws IOException {
        //1. 创建请求对象: 查询索引库
        GetIndexRequest request = new GetIndexRequest("shopping");
        //2. 客户端执行请求发送,返回响应对象
        GetIndexResponse getIndexResponse = restHighLevelClient.indices().get(request, RequestOptions.DEFAULT);
        //3. 打印结果信息
        System.out.println("aliases: "+getIndexResponse.getAliases());
        System.out.println("mapping:"+getIndexResponse.getMappings());
        System.out.println("settings: "+getIndexResponse.getSettings());
        //4. 关闭客户端,释放连接资源
        restHighLevelClient.close();
    }
    //删除索引库
    @Test
    public void deleteIndex() throws IOException {
        //1. 创建请求对象
        DeleteIndexRequest request = new DeleteIndexRequest("shopping01");
        //2. 客户端发送请求,获取响应对象
        AcknowledgedResponse deleteResponse = restHighLevelClient.indices().delete(request, RequestOptions.DEFAULT);
        //3. 打印结果信息
        System.out.println("ack:"+deleteResponse.isAcknowledged());
        //4. 关闭客户端,释放连接资源
        restHighLevelClient.close();
    }
}
