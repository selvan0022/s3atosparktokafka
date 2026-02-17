package com.spark.test.spark;

/**
 * Configuration class for S3 access
 */
public class S3Configuration {
    private String accessKey;
    private String secretKey;
    private String s3Path;
    private String endpoint;

    /**
     * Constructor
     * @param accessKey AWS access key
     * @param secretKey AWS secret key
     * @param s3Path S3 bucket path (e.g., s3a://bucket-name/path/to/file)
     * @param endpoint S3 endpoint (e.g., s3.us-east-1.amazonaws.com)
     */
    public S3Configuration(String accessKey, String secretKey, String s3Path, String endpoint) {
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.s3Path = s3Path;
        this.endpoint = endpoint;
    }

    public String getAccessKey() {
        return accessKey;
    }

    public void setAccessKey(String accessKey) {
        this.accessKey = accessKey;
    }

    public String getSecretKey() {
        return secretKey;
    }

    public void setSecretKey(String secretKey) {
        this.secretKey = secretKey;
    }

    public String getS3Path() {
        return s3Path;
    }

    public void setS3Path(String s3Path) {
        this.s3Path = s3Path;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    @Override
    public String toString() {
        return "S3Configuration{" +
                "accessKey='" + (accessKey != null ? "***" : "null") + '\'' +
                ", secretKey='" + (secretKey != null ? "***" : "null") + '\'' +
                ", s3Path='" + s3Path + '\'' +
                ", endpoint='" + endpoint + '\'' +
                '}';
    }
}
