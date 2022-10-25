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
import org.junit.platform.commons.util.StringUtils;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * @author yangwei
 */
public class ProduceUserPlaySongLog {
    public static void main(String[] args) throws Exception {
        HttpClient client = HttpClients.createDefault();
        HttpPost post = new HttpPost(Constant.URL + LogType.LOG_TYPE_USER_PLAY_SONG);
        post.setHeader("Content-Type", "application/json");
        while (true) {
            try (InputStream is = ProduceUserPlaySongLog.class.getClassLoader().getResourceAsStream("client_play_song_infos");
                 BufferedReader br = new BufferedReader(new InputStreamReader(Objects.requireNonNull(is)))) {
                String line = br.readLine();
                while (StringUtils.isNotBlank(line)) {
                    post.setEntity(new StringEntity("["+line+"]", Charset.forName("UTF-8")));
                    TimeUnit.MILLISECONDS.sleep(200);
                    HttpResponse response = client.execute(post);
                    if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                        final String result = EntityUtils.toString(response.getEntity(), "utf-8");
                        System.out.println("==>> result: " + result);
                    }
                    line = br.readLine();
                }
            }
        }
    }
}
