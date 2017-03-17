import java.io.FileInputStream;
import java.net.InetAddress;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.*;
import org.elasticsearch.client.transport.*;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;


public class ESClient {
	
	public Properties prop = new Properties();
	
	void initConfig(){
		FileInputStream in;
		try {
			in = new FileInputStream("config.properties");
			prop.load(in);
			in.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}				
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		ESClient esc = new ESClient();
		esc.initConfig();
		String host = esc.prop.getProperty("es.host");
		int port = Integer.parseInt(esc.prop.getProperty("es.port"));
		
		Settings settings = Settings.settingsBuilder()
		        .put("client.transport.sniff", true).put("cluster.name", "es23_t").build();
		TransportClient client = null;
		try{
			client = TransportClient.builder().settings(settings).build()
				.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port));		
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		Map<String, Object> json = new HashMap<String, Object>();
		json.put("user","kimchy");
		json.put("postDate",new Date());
		json.put("message","trying out Elasticsearch");
		
		IndexResponse response = client.prepareIndex("helloworld", "helloworld")
		        .setSource(json)
		        .get();
		
		String _id = response.getId();
		
		
		GetResponse res = client.prepareGet("helloworld", "helloworld", _id).get();

		client.close();
		
		System.out.println(res.getId());
	}

}
