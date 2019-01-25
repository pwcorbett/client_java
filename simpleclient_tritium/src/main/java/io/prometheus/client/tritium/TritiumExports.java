/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
 */

package io.prometheus.client.tritium;

import com.codahale.metrics.*;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.palantir.logsafe.SafeArg;
import com.palantir.tritium.metrics.registry.MetricName;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;
import io.prometheus.client.Collector;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Collect Metrics from a Tritium Registry
 */
public class TritiumExports extends Collector implements Collector.Describable {

    private static final Logger LOGGER = LoggerFactory.getLogger(TritiumExports.class);
    private static final String REGISTRY_TYPE = "Tritium";
    private static final String UNDERSCORE_NANOSECONDS = "_nanoseconds";
    private static final String UNDERSCORE_TOTAL = "_total";
    private static final String UNDERSCORE_COUNT = "_count";
    private static final String UNDERSCORE_MIN = "_min";
    private static final String UNDERSCORE_MAX = "_max";
    private static final String UNDERSCORE_MEAN = "_mean";
    private static final String UNDERSCORE_STDDEV = "_stddev";
    private static final String QUANTILE = "quantile";
    private static final String ZERO_POINT_5 = "0.5";
    private static final String ZERO_POINT_75 = "0.75";
    private static final String ZERO_POINT_95 = "0.95";
    private static final String ZERO_POINT_98 = "0.98";
    private static final String ZERO_POINT_99 = "0.99";
    private static final String ZERO_POINT_999 = "0.999";

    private static final List<String> QUANTILES = Lists.newArrayList(
            ZERO_POINT_5, ZERO_POINT_75, ZERO_POINT_95,
            ZERO_POINT_98, ZERO_POINT_99, ZERO_POINT_999
    );

    private static final String HELP_MESSAGE_FORMAT = "Generated from %s metric import (metric=%s, type=%s)";
    private static final Pattern METRIC_NAME_RE = Pattern.compile("[^a-zA-Z0-9_]");
    private static final char c_underscore = '_';
    private static final String s_underscore = "_";
    private static final String invalid_metric_name = "invalid_prometheus_metric_or_label_name";

    private final TaggedMetricRegistry registry;


    /**
     * Instantiate a TaggedMetricRegistryExports and use the supplied registry.
     * @param registry a metric registry to export in prometheus.
     */
    public TritiumExports(TaggedMetricRegistry registry) {
        this.registry = registry;
    }

    @Override
    public final List<MetricFamilySamples> describe() {
        return Lists.newArrayList();
    }

    @Override
    public final List<MetricFamilySamples> collect() {
        Map<String, MetricFamilySamples> mfSamples = Maps.newHashMap();

        for (SortedMap.Entry<MetricName, Metric> entry : registry.getMetrics().entrySet()) {
            final Metric value = entry.getValue();
            List<MetricFamilySamples> samples;

            if (value instanceof Counter) {
                samples = fromCounter(entry.getKey(), (Counter) value);
            } else  if (value instanceof Gauge) {
                samples = fromGauge(entry.getKey(), (Gauge<?>) value);
            } else  if (value instanceof Histogram) {
                samples = fromHistogram(entry.getKey(), (Histogram) value);
            } else  if (value instanceof Timer) {
                samples = fromTimer(entry.getKey(), (Timer) value);
            } else  if (value instanceof Meter) {
                samples = fromMeter(entry.getKey(), (Meter) value);
            } else {
                LOGGER.warn("Unexpected type for Metric {}: {}",
                        SafeArg.of("name", entry.getKey().safeName()),
                        SafeArg.of("class", value == null ? "null" : value.getClass().getName()));
                continue;
            }
            merge(mfSamples, samples);
        }

        return Lists.newArrayList(mfSamples.values());
    }

    /**
     * Generate the help message based on the metric name and type.
     * @param metricName The metric name to use.
     * @param metric The metric type being mapped.
     * @return a help message.
     */
    private String getHelpMessage(String metricName, Metric metric) {
        return String.format(HELP_MESSAGE_FORMAT, REGISTRY_TYPE, metricName, metric.getClass().getName());
    }

    /**
     * Generate the label name/value pairs from the tags.
     * Dropping the labels that have no value.
     */
    private static Pair<List<String>, List<String>> generateLabelPairs(final Map<String, String> tags) {
        final List<String> labels = Lists.newLinkedList();
        final List<String> values = Lists.newArrayList();

        for (final Map.Entry<String, String> entry : tags.entrySet()) {
            if (StringUtils.isBlank(entry.getKey()) || StringUtils.isBlank(entry.getValue())) {
                continue;
            }
            labels.add(sanitizeMetricName(entry.getKey()));
            values.add(entry.getValue());
        }
        return Pair.of(labels, values);
    }


    /**
     * Export counter as Prometheus <a href="https://prometheus.io/docs/concepts/metric_types/#gauge">Gauge</a>.
     */
    private List<MetricFamilySamples> fromCounter(MetricName metric, Counter counter) {
        final Pair<List<String>, List<String>> tags = generateLabelPairs(metric.safeTags());
        final String name = sanitizeMetricName(metric.safeName());
        final Collector.MetricFamilySamples.Sample sample = new Collector.MetricFamilySamples.Sample(name,
                tags.getKey(), tags.getValue(), Long.valueOf(counter.getCount()).doubleValue());
        return Lists.newArrayList(new Collector.MetricFamilySamples(name, Collector.Type.GAUGE,
                getHelpMessage(metric.safeName(), counter), Lists.newArrayList(sample)));
    }

    /**
     * Export gauge as a prometheus gauge.
     */
    private List<MetricFamilySamples> fromGauge(MetricName metric, Gauge<?> gauge) {
        final Pair<List<String>, List<String>> tags = generateLabelPairs(metric.safeTags());
        final String name = sanitizeMetricName(metric.safeName());
        final Object obj = gauge.getValue();
        double value;
        if (obj instanceof Number) {
            value = ((Number) obj).doubleValue();
        } else if (obj instanceof Boolean) {
            value = ((Boolean) obj) ? 1 : 0;
        } else {
            LOGGER.trace("Invalid type for Gauge {}: {}", SafeArg.of("name", name),
                    SafeArg.of("class", obj == null ? "null" : obj.getClass().getName()));
            return Lists.newArrayList();
        }
        final Collector.MetricFamilySamples.Sample sample = new Collector.MetricFamilySamples.Sample(name,
                tags.getKey(), tags.getValue(), value);
        return Lists.newArrayList(new Collector.MetricFamilySamples(name, Collector.Type.GAUGE,
                getHelpMessage(metric.safeName(), gauge), Lists.newArrayList(sample)));
    }

    /**
     * Export a histogram snapshot as a prometheus SUMMARY.
     *
     * @param name metric name.
     * @param snapshot the histogram snapshot.
     * @param count the total sample count for this snapshot.
     * @param factor a factor to apply to histogram values.
     *
     */
    private List<Collector.MetricFamilySamples> fromSnapshotAndCount(
            String name, Snapshot snapshot,
            long count, double factor, String helpMessage,
            List<String> labels, List<String> labelValues) {
        final List<Collector.MetricFamilySamples.Sample> samples = Lists.newArrayList();

        labels.add(0, QUANTILE);
        labelValues.add(0, "");

        for (final String quantile : QUANTILES) {
            labelValues.set(0, quantile);
            samples.add(new Collector.MetricFamilySamples.Sample(name,
                    Lists.newArrayList(labels), Lists.newArrayList(labelValues),
                    snapshot.getValue(Double.valueOf(quantile)) * factor));
        }

        labels.remove(0);
        labelValues.remove(0);

        samples.add(new Collector.MetricFamilySamples.Sample(name + UNDERSCORE_COUNT, labels, labelValues,
                count));
        samples.add(new Collector.MetricFamilySamples.Sample(name + UNDERSCORE_MIN, labels, labelValues,
                snapshot.getMin()));
        samples.add(new Collector.MetricFamilySamples.Sample(name + UNDERSCORE_MAX, labels, labelValues,
                snapshot.getMax()));
        samples.add(new Collector.MetricFamilySamples.Sample(name + UNDERSCORE_MEAN, labels, labelValues,
                snapshot.getMean()));
        samples.add(new Collector.MetricFamilySamples.Sample(name + UNDERSCORE_STDDEV, labels, labelValues,
                snapshot.getStdDev()));

        return Lists.newArrayList(
                new Collector.MetricFamilySamples(name, Collector.Type.SUMMARY, helpMessage, samples));
    }

    /**
     * Convert histogram snapshot.
     */
    private List<MetricFamilySamples> fromHistogram(MetricName metric, Histogram histogram) {
        final Pair<List<String>, List<String>> tags = generateLabelPairs(metric.safeTags());
        final String name = sanitizeMetricName(metric.safeName());
        return fromSnapshotAndCount(name, histogram.getSnapshot(), histogram.getCount(), 1.0,
                getHelpMessage(metric.safeName(), histogram), tags.getKey(), tags.getValue());
    }

    /**
     * Export Timer as a histogram. Use TIME_UNIT as time unit.
     */
    private List<MetricFamilySamples> fromTimer(MetricName metric, Timer timer) {
        final Pair<List<String>, List<String>> tags = generateLabelPairs(metric.safeTags());
        final String name = sanitizeMetricName(metric.safeName());
        return fromSnapshotAndCount(name, timer.getSnapshot(), timer.getCount(),
                1.0D / TimeUnit.SECONDS.toNanos(1L),
                getHelpMessage(metric.safeName(), timer), tags.getKey(), tags.getValue());
    }

    /**
     * Export a Meter as as prometheus COUNTER.
     */
    private List<MetricFamilySamples> fromMeter(MetricName metric, Meter meter) {
        final Pair<List<String>, List<String>> tags = generateLabelPairs(metric.safeTags());
        final String name = sanitizeMetricName(metric.safeName()) + UNDERSCORE_TOTAL;
        return Lists.newArrayList(
                new Collector.MetricFamilySamples(name, Collector.Type.COUNTER,
                        getHelpMessage(metric.safeName(), meter),
                        Lists.newArrayList(
                                new Collector.MetricFamilySamples.Sample(name,
                                        tags.getKey(), tags.getValue(), meter.getCount())))

        );
    }

    /**
     * Merge Collector.MetricFamilySamples
     * @param mfSamples Collector.MetricFamilySamples
     * @param samples Collector.MetricFamilySamples to add.
     */
    private void merge(final Map<String, Collector.MetricFamilySamples> mfSamples,
                      List<Collector.MetricFamilySamples> samples) {
        for (final Collector.MetricFamilySamples sample : samples) {
            if (mfSamples.containsKey(sample.name)) {
                mfSamples.get(sample.name).samples.addAll(sample.samples);
            } else {
                mfSamples.put(sample.name, sample);
            }
        }
    }



    /**
     * Sanitizes a string so that it matches Prometheus's data model.
     * Input strings will be transformed to match the regular expression [a-z0-9_]*. Uppercase
     * characters in the input string are converted to lowercase, and all other characters that are not
     * numbers or already lowercase letters will be converted to an underscore delimiter. Surrounding
     * delimiters in the resulting transformed string are trimmed before being returned.
     * Returns an InvalidName if the input string is numeric or otherwise cannot be sanitized to match
     * Prometheus's data model.
     *
     * @param metricName
     *            original metric name
     * @return the sanitized metric name
     */
    public static String sanitizeMetricName(String metricName) {
        String name = METRIC_NAME_RE.matcher(metricName).replaceAll("_").toLowerCase();

        //no leading underscore or digit
        while (name.length() > 0 && (name.charAt(0) == c_underscore || Character.isDigit(name.charAt(0)))) {
            name = name.substring(1, name.length());
        }

        //no trailing underscore
        while (name.length() > 0 && name.endsWith(s_underscore)) {
            name = name.substring(0, name.length() - 1);
        }

        // This can only happen if an input string contains all non-alphabetic characters or is empty.
        if (name.length() == 0) {
            return invalid_metric_name;
        }

        return name;
    }
}
