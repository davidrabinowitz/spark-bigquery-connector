package com.google.cloud.spark.bigquery.it;

public class IntegrationTestUtils {

    static String safeGetEnv(String envVar) {
        String result = System.getenv(envVar);
        if ( result != null) {
            return result;
        } else {
            throw new IllegalArgumentException("Missing environment variable " + envVar);
        }
    }
}
