package io.debezium.connector.db2as400;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

import com.ibm.as400.access.AS400;

import io.debezium.config.Configuration;

public class As400RpcConnector extends SourceConnector {
	private Map<String, String> props;
	
	public As400RpcConnector() {
		System.out.println("As400RpcConnector constructed");
	}
	
    public As400RpcConnector(Configuration config) {

    }

	@Override
	public String version() {
		return Module.version();
	}

	@Override
	public void start(Map<String, String> props) {
		this.props = props;		
	}

	@Override
	public Class<? extends Task> taskClass() {
		return As400ConnectorTask.class;
	}

	@Override
	public List<Map<String, String>> taskConfigs(int maxTasks) {
		// TODO Auto-generated method stub
		 List<Map<String, String>> l = new ArrayList<>();
		 l.add(props);
		 return l;
	}

	@Override
	public void stop() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public ConfigDef config() {
		// TODO Auto-generated method stub
		return null;
	}

}
