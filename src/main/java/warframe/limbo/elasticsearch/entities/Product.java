package warframe.limbo.elasticsearch.entities;

import lombok.*;

@AllArgsConstructor
@NoArgsConstructor
@Data
@ToString
@Builder//通过工厂构建对象
public class Product {
    private Long id;//商品唯一标识
    private String title;//商品名称
    private Double price;//商品价格
    private String images;//图片地址
}
