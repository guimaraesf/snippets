package br.com.bs.bigtable.config;

import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import java.io.IOException;

@Profile("bigtable")
@Configuration
@ConfigurationProperties("app.bigtable")
public class BigTableConfig {

	private final Logger LOGGER = LoggerFactory.getLogger(getClass());

	private String projectId;
	private String instanceId;
	private String tableProduto;
	private String tablePrefix;
	private String columnFamily;
	private String appProfile;
	private int limitRows;
	private BigtableDataSettings settings;

	@Bean
	public BigtableDataClient buildConnection() throws IOException {
		this.settings = BigtableDataSettings.newBuilder().setProjectId(this.projectId)
				.setInstanceId(this.instanceId).setAppProfileId(this.appProfile).build();
		BigtableDataClient dataClient = BigtableDataClient.create(this.settings);
		return dataClient;
	}

	public void onStart(){LOGGER.info("BigTable Limit Rows: "+ getLimitRows());}

	public String getProjectId() {
		return projectId;
	}

	public void setProjectId(String projectId) {
		this.projectId = projectId;
	}

	public String getInstanceId() {
		return instanceId;
	}

	public void setInstanceId(String instanceId) {
		this.instanceId = instanceId;
	}

    public String getAppProfile() {
		return appProfile;
	}

	public void setAppProfile(String appProfile) {
		this.appProfile = appProfile;
	}

	public String getTableProduto() {
		return tableProduto;
	}

	public void setTableProduto(String tableProduto) {
		this.tableProduto = tableProduto;
	}

	public String getTablePrefix() {
		return tablePrefix;
	}

	public void setTablePrefix(String tablePrefix) {
		this.tablePrefix = tablePrefix;
	}

	public String getColumnFamily() {
		return columnFamily;
	}

	public void setColumnFamily(String columnFamily) {
		this.columnFamily = columnFamily;
	}

	public void setLimitRows(int limitRows) {
		this.limitRows = limitRows;
	}

	public int getLimitRows() {
		return limitRows;
	}

	public String getTableName(String context, String table){
    	return getTablePrefix() + context + table;
	}

	public String getTableName(String table){
    	return getTableName("", table);
	}

}
