package com.movierecommendation.hbase;

import java.io.IOException;

import javax.annotation.PostConstruct;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.springframework.stereotype.Component;

@Component
public class HBaseClientConfiguration {

	private Connection connection;
	private Admin admin;
	
//	@PostConstruct
	public void init() throws IOException {
		Configuration config = HBaseConfiguration.create();

        try (Connection connection = ConnectionFactory.createConnection(config);
             Admin admin = connection.getAdmin()) {
        	this.connection = connection;
        	this.admin = admin;
        }
	}

	public Connection getConnection() {
		return connection;
	}

	public Admin getAdmin() {
		return admin;
	}
	
}
