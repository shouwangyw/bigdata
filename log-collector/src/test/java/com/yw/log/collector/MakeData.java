package com.yw.log.collector;

import com.yw.log.collector.common.Constant;
import com.yw.log.collector.common.constant.LogType;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.nio.charset.Charset;
import java.util.concurrent.TimeUnit;

/**
 * 模拟访问数据采集端口，数据采集端口采集数据
 * @author yangwei
 */
public class MakeData {
    public static void main(String[] args) throws Exception {
        HttpClient client = HttpClients.createDefault();
        int i = 0;
        while (i < 10) {
            HttpPost post = new HttpPost(Constant.URL + LogType.LOG_TYPE_USER_LOGIN);
            post.setHeader("Content-Type", "application/json");
            String body = "[{'id':"+i+",'name':'zhangsan','age':14,'gender':'男'},{'name':'lisi','age':15,'gender':'女'}]";
            post.setEntity(new StringEntity(body, Charset.forName("UTF-8")));
            TimeUnit.SECONDS.sleep(2);
            i++;
            HttpResponse response = client.execute(post);
            if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                System.out.println(EntityUtils.toString(response.getEntity(), "utf-8"));
            }
        }
    }
}
