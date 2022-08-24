package test.org.apache.spark.sql.connector.metric;

import org.apache.spark.sql.connector.metric.CustomSumMetric;

public class MySumMetric extends CustomSumMetric {
    public String aggregateTaskMetricsUseLoop(long[] taskMetrics) {
        long sum = 0L;
        for (long taskMetric : taskMetrics) {
            sum += taskMetric;
        }
        return String.valueOf(sum);
    }

    @Override
    public String name() {
        return "CustomSumMetric";
    }

    @Override
    public String description() {
        return "Sum up CustomMetric";
    }
}
