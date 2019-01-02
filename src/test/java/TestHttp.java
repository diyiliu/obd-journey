import com.tiza.util.JacksonUtil;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Description: TestHttp
 * Author: DIYILIU
 * Update: 2018-08-07 15:44
 */
public class TestHttp {

    @Test
    public void test() throws Exception {

        String url = "https://restapi.amap.com/v3/geocode/regeo";
        URIBuilder builder = new URIBuilder(url);

        builder.setParameter("key", "e6f07e683b599f2d9e4c7f79073d1c7b");
        builder.setParameter("output", "json");
        builder.setParameter("location", "117.26535845,34.28686242");

        HttpGet httpGet = new HttpGet(builder.build());
        HttpClient client = new DefaultHttpClient();

        HttpResponse response = client.execute(httpGet);
        HttpEntity entity = response.getEntity();
        String result = EntityUtils.toString(entity, "UTF-8");
        System.out.println(result);

        Map map = JacksonUtil.toObject(result, HashMap.class);
        int status = Integer.parseInt(String.valueOf(map.get("status")));
        if (status == 1) {
            Map regeo = (Map) map.get("regeocode");
            Map compoment = (Map) regeo.get("addressComponent");

            System.out.println(regeo.get("formatted_address"));
            System.out.println(compoment.get("province"));
            System.out.println(compoment.get("city"));
            System.out.println(compoment.get("district"));
        }
    }
}
