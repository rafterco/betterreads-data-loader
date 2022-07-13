package com.betterreads.data.connection;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.io.File;

@ConfigurationProperties(prefix = "datastax.astra")
public class DataStaxAstraProperties {

    private File raftaFile;

    private File secureConnectBundle;

    private String raftaProps;

    public File getSecureConnectBundle() {
        return secureConnectBundle;
    }

    public String getRaftaProps() {
        return raftaProps;
    }

    public void setSecureConnectBundle(File secureConnectBundle) {
        this.secureConnectBundle = secureConnectBundle;
    }

    public void setRaftaProps(String raftaProps) {
        this.raftaProps = raftaProps;
    }

    public void setRaftaFile(File raftaFile) {this.raftaFile = raftaFile;}

    public File getRafaFile() {
        return this.raftaFile;
    }
}
