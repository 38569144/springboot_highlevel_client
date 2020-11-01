package warframe.limbo.elasticsearch;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import warframe.limbo.elasticsearch.entities.Product;

import javax.annotation.Resource;
import java.io.IOException;

@RunWith(SpringRunner.class)
@SpringBootTest
public class DocumentTest {
    @Resource
    private RestHighLevelClient client;
    //新增文档
    @Test
    public void saveDoc() throws IOException {
        //1. 创建请求对象(创建索引库CreateIndexRequest),索引库名称,类型名称,主键id
        IndexRequest request = new IndexRequest().index("shopping01").type("_doc").id("2");
        //方式1: 写一个Product对象将对象转为json字符串
        Product product = Product.builder().id(1L).title("小米手机").price(1999.0).build();
        ObjectMapper objectMapper = new ObjectMapper();
        String productJson = objectMapper.writeValueAsString(product);
        request.source(productJson, XContentType.JSON);
        //方式2: 直接在source中写入key-value参数
        //request.source(XContentType.JSON,"id",2L,"title","小米手机2.0","price","3999.0");
        //2. 客户端发送请求,获取响应对象
        IndexResponse response = client.index(request, RequestOptions.DEFAULT);
        //3. 打印结果信息
        System.out.println("_index:"+response.getIndex());
        System.out.println("_type:"+response.getType());
        System.out.println("_id:"+response.getId());
        System.out.println("_result:"+response.getResult());
        //4. 关闭客户端,释放连接
        client.close();
    }
    //修改文档
    @Test
    public void update() throws IOException {
        //1. 创建请求对象(创建索引库CreateIndexRequest),索引库名称,类型名称,主键id
        UpdateRequest request = new UpdateRequest().index("shopping01").type("_doc").id("2");
        //设置请求体
        request.doc(XContentType.JSON,"id","2","price",29999.00,"title","小米手机4.0");
        //2. 客户端发送请求,获取响应对象
        UpdateResponse response = client.update(request, RequestOptions.DEFAULT);
        //3. 打印结果信息
        System.out.println("_index:"+response.getIndex());
        System.out.println("_type:"+response.getType());
        System.out.println("_id:"+response.getId());
        System.out.println("_result:"+response.getResult());
        //4. 关闭客户端,释放连接
        client.close();
    }
    //查询文档
    @Test
    public void getDoc() throws IOException {
        //1. 创建请求对象
        GetRequest request = new GetRequest().index("shopping01").type("_doc").id("4");
        //2. 客户端发送请求,获取响应对象
        GetResponse response = client.get(request, RequestOptions.DEFAULT);
        //3. 打印结果信息
        System.out.println("_index:"+response.getIndex());
        System.out.println("_type:"+response.getType());
        System.out.println("_id:"+response.getId());
        System.out.println("_source:"+response.getSourceAsString());
        //4. 关闭客户端,释放连接
        client.close();
    }
    //删除文档
    @Test
    public void deleteDoc() throws IOException {
        //1. 创建请求对象
        DeleteRequest request = new DeleteRequest().index("shopping01").type("_doc").id("2");
        //2. 客户端发送请求,获取响应对象
        DeleteResponse response = client.delete(request, RequestOptions.DEFAULT);
        //3. 打印结果信息
        System.out.println(response.toString());
        System.out.println("_index:"+response.getIndex());
        System.out.println("_type:"+response.getType());
        System.out.println("_id:"+response.getId());
        System.out.println("_result:"+response.getResult());
        //4. 关闭客户端,释放连接
        client.close();
    }
    //批量新增操作
    @Test
    public void bulkSave() throws IOException {
        //1. 创建请求对象
        BulkRequest request = new BulkRequest();
        request.add(new IndexRequest().index("shopping01").type("_doc").id("3").source(XContentType.JSON,"title", "小米手机"));
        request.add(new IndexRequest().index("shopping01").type("_doc").id("4").source(XContentType.JSON,"title", "苹果手机"));
        request.add(new IndexRequest().index("shopping01").type("_doc").id("5").source(XContentType.JSON,"title", "锤子手机"));
        request.add(new IndexRequest().index("shopping01").type("_doc").id("6").source(XContentType.JSON,"title", "华为手机"));
        //2. 客户端发送请求,获取响应对象
        BulkResponse response = client.bulk(request, RequestOptions.DEFAULT);
        //3. 打印结果信息
        System.out.println("took:"+response.getTook());
        System.out.println("items:"+response.getItems());
        //4. 关闭客户端,释放连接
        client.close();
    }
    //批量删除操作
    @Test
    public void bulkDelete() throws IOException {
        //1. 创建请求对象
        BulkRequest request = new BulkRequest();
        request.add(new DeleteRequest().index("shopping01").type("_doc").id("4"));
        request.add(new DeleteRequest().index("shopping01").type("_doc").id("2"));
        request.add(new DeleteRequest().index("shopping01").type("_doc").id("3"));
        //2. 客户单发送请求,获取响应对象
        BulkResponse response = client.bulk(request, RequestOptions.DEFAULT);
        //3. 打印输出结果
        System.out.println("took:"+response.getTook());
        System.out.println("items"+response.getItems());
        //4. 关闭连接
        client.close();
    }

}
