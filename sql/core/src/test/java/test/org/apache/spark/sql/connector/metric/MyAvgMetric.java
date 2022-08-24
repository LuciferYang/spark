package test.org.apache.spark.sql.connector.metric;

import org.apache.spark.sql.connector.metric.CustomAvgMetric;

import java.text.DecimalFormat;

public class MyAvgMetric extends CustomAvgMetric {
    public String aggregateTaskMetricsUseLoop(long[] taskMetrics) {
        if (taskMetrics.length > 0) {
            long sum = 0L;
            for (long taskMetric : taskMetrics) {
                sum += taskMetric;
            }
            double average = ((double) sum) / taskMetrics.length;
            return new DecimalFormat("#0.000").format(average);
        } else {
            return "0";
        }
    }

    @Override
    public String name() {
        return "CustomAvgMetric";
    }

    @Override
    public String description() {
        return "Average CustomMetric";
    }
}
