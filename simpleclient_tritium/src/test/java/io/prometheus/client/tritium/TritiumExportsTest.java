package io.prometheus.client.tritium;

import com.codahale.metrics.*;
import com.codahale.metrics.Timer;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.palantir.tritium.metrics.registry.DefaultTaggedMetricRegistry;
import com.palantir.tritium.metrics.registry.MetricName;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.common.TextFormat;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;


public class TritiumExportsTest {

    private static final String NAME_1 = "name_1";
    private static final String NAME_2 = "name_2";
    private static final String NAME_3 = "name_3";
    private static final String NAME_4 = "name_4";
    private static final String NAME_5 = "name_5";
    private static final String KEY_1 = "key_1";
    private static final String KEY_2 = "key_2";
    private static final String VAL_1 = "val_1";
    private static final String VAL_2 = "val_2";

    private static final MetricName METRIC_1 = MetricName.builder().safeName(NAME_1).build();
    private static final MetricName METRIC_2 = MetricName.builder().safeName(NAME_2)
            .putSafeTags(KEY_1, VAL_1).build();
    private static final MetricName METRIC_3 = MetricName.builder().safeName(NAME_3)
            .putSafeTags(KEY_1, VAL_1).putSafeTags(KEY_2, VAL_2).build();
    private static final MetricName METRIC_4 = MetricName.builder().safeName(NAME_4).build();
    private static final MetricName METRIC_5 = MetricName.builder().safeName(NAME_5).build();

    private CollectorRegistry registry = new CollectorRegistry();
    private TaggedMetricRegistry metricRegistry;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setUp() {
        metricRegistry =  new DefaultTaggedMetricRegistry();
        new TritiumExports(metricRegistry).register(registry);
    }

    private String getOutput() throws IOException {
        final StringWriter writer = new StringWriter();
        TextFormat.write004(writer, registry.metricFamilySamples());
        return writer.toString();
    }

    @Test
    public void testCounter() throws IOException {
        metricRegistry.counter(METRIC_1);
        metricRegistry.counter(METRIC_1).inc();

        metricRegistry.counter(METRIC_2);
        metricRegistry.counter(METRIC_2).inc();

        metricRegistry.counter(METRIC_3);
        metricRegistry.counter(METRIC_3).inc();

        assertEquals(Double.valueOf(1), registry.getSampleValue(METRIC_1.safeName()));
        assertEquals(Double.valueOf(1), registry.getSampleValue(METRIC_2.safeName(),
                METRIC_2.safeTags().keySet().toArray(new String[]{}),
                METRIC_2.safeTags().values().toArray(new String[]{}))
        );
        assertEquals(Double.valueOf(1), registry.getSampleValue(METRIC_3.safeName(),
                METRIC_3.safeTags().keySet().toArray(new String[]{}),
                METRIC_3.safeTags().values().toArray(new String[]{}))
        );

        String output = getOutput();

        assertNotNull(output);
        assertTrue(output.contains("name_1 1.0"));
        assertTrue(output.contains("name_2{key_1=\"val_1\",} 1.0"));
        assertTrue(output.contains("name_3{key_1=\"val_1\",key_2=\"val_2\",} 1.0"));
    }

    @Test
    public void testGuage() throws IOException {
        Gauge<Integer> integerGauge = () -> 1234;
        Gauge<Long> longGauge = () -> 1234L;
        Gauge<Double> doubleGauge = () -> 1.234D;
        Gauge<Float> floatGauge = () -> 0.1234F;
        Gauge<Boolean> booleanGauge = () -> true;

        metricRegistry.gauge(METRIC_1, integerGauge);
        metricRegistry.gauge(METRIC_2, longGauge);
        metricRegistry.gauge(METRIC_3, doubleGauge);
        metricRegistry.gauge(METRIC_4, floatGauge);
        metricRegistry.gauge(METRIC_5, booleanGauge);

        assertEquals(Double.valueOf(1234), registry.getSampleValue(METRIC_1.safeName()));
        assertEquals(Double.valueOf(1234), registry.getSampleValue(METRIC_2.safeName(),
                METRIC_2.safeTags().keySet().toArray(new String[]{}),
                METRIC_2.safeTags().values().toArray(new String[]{}))
        );
        assertEquals(Double.valueOf(1.234), registry.getSampleValue(METRIC_3.safeName(),
                METRIC_3.safeTags().keySet().toArray(new String[]{}),
                METRIC_3.safeTags().values().toArray(new String[]{}))
        );
        assertEquals(Double.valueOf(0.1234F), registry.getSampleValue(METRIC_4.safeName()));
        assertEquals(Double.valueOf(1), registry.getSampleValue(METRIC_5.safeName()));

        String output = getOutput();

        assertNotNull(output);
        assertTrue(output.contains("name_1 1234.0"));
        assertTrue(output.contains("name_2{key_1=\"val_1\",} 1234.0"));
        assertTrue(output.contains("name_3{key_1=\"val_1\",key_2=\"val_2\",} 1.234"));
        assertTrue(output.contains("name_4 0.1234"));
        assertTrue(output.contains("name_5 1.0"));
    }


    @Test
    public void testInvalidGaugeValue() {
        Gauge<String> invalidGauge = new Gauge<String>() {
            @Override
            public String getValue() {
                return "invalid";
            }
        };

        metricRegistry.gauge(METRIC_1, invalidGauge);
        assertEquals(null, registry.getSampleValue(METRIC_1.safeName()));
    }

    @Test
    public void testNullGaugeValue() {
        Gauge<String> invalidGauge = new Gauge<String>() {
            @Override
            public String getValue() {
                return null;
            }
        };
        metricRegistry.gauge(METRIC_1, invalidGauge);
        assertEquals(null, registry.getSampleValue(METRIC_1.safeName()));
    }

    @Test
    public void testHistogram() {
        Histogram histogram = metricRegistry.histogram(METRIC_1);
        Double total = 100d;
        for (int i = 0; i < total; i++) {
            histogram.update(i);
        }
        assertEquals(total, registry.getSampleValue(METRIC_1.safeName() + "_count"));
        for (Double d : Arrays.asList(0.75, 0.95, 0.98, 0.99)) {
            assertEquals(Double.valueOf((d - 0.01) * 100), registry.getSampleValue(METRIC_1.safeName(),
                    new String[]{"quantile"}, new String[]{d.toString()}));
        }

        Histogram histogram2 = metricRegistry.histogram(METRIC_2);
        for (int i = 0; i < total; i++) {
            histogram2.update(i);
        }
        assertEquals(total, registry.getSampleValue(METRIC_2.safeName() + "_count",
                METRIC_2.safeTags().keySet().toArray(new String[]{}),
                METRIC_2.safeTags().values().toArray(new String[]{})));
        for (Double d : Arrays.asList(0.75, 0.95, 0.98, 0.99)) {
            List<String> keys = Lists.newArrayList("quantile");
            keys.addAll(METRIC_2.safeTags().keySet());
            List<String> values = Lists.newArrayList(d.toString());
            values.addAll(METRIC_2.safeTags().values());
            assertEquals(Double.valueOf((d - 0.01) * 100), registry.getSampleValue(METRIC_2.safeName(),
                    keys.toArray(new String[]{}), values.toArray(new String[]{})));
        }
    }


    @Test
    public void testMeter() throws IOException {
        Meter meter = metricRegistry.meter(METRIC_1);
        meter.mark();
        meter.mark();
        String name = METRIC_1.safeName() + "_total";
        assertEquals(Double.valueOf(2), registry.getSampleValue(name));

        String output = getOutput();

        assertNotNull(output);
        assertTrue(output.contains(name + " 2.0"));
    }

    @Test
    public void testTimer() throws IOException, InterruptedException {
        Timer t = metricRegistry.timer(METRIC_1);
        Timer.Context time = t.time();
        Thread.sleep(1L);
        time.stop();

        String output = getOutput();

        assertTrue(registry.getSampleValue(METRIC_1.safeName(), new String[]{"quantile"}, new String[]{"0.99"}) > 0.001);
        assertEquals(Double.valueOf(1.0), registry.getSampleValue(METRIC_1.safeName() + "_count"));


        assertNotNull(output);
        assertTrue(output.contains(METRIC_1.safeName() + "_count 1.0"));
    }

    @Test
    public void testMetricNames() throws IOException {
        Map<String, String> names = Maps.newHashMap();
        names.put("service-name", "service_name");
        names.put("Stack", "stack");
        names.put("$Host", "host");
        names.put("32metricName", "metricname");
        names.put("3metricName", "metricname");
        names.put("3metr1c", "metr1c");
        names.put("1234", "invalid_prometheus_metric_or_label_name");
        names.put("response_code_", "response_code");
        names.put("dirty:Label", "dirty_label");
        names.put("foo.service", "foo_service");
        names.put("myservice", "myservice");

        for (Map.Entry<String, String> name : names.entrySet()) {
            MetricName metricName = MetricName.builder().safeName(name.getKey()).build();
            metricRegistry.counter(metricName);
            metricRegistry.counter(metricName).inc();

            assertNotNull(registry.getSampleValue(name.getValue()));
            assertEquals(Double.valueOf(1), registry.getSampleValue(name.getValue()));
        }

        MetricName metricName = MetricName.builder().safeName("metric_name").putSafeTags("1234", "1234").build();
        metricRegistry.counter(metricName);
        metricRegistry.counter(metricName).inc();

        assertNotNull(registry.getSampleValue(metricName.safeName(),
                new String[]{"invalid_prometheus_metric_or_label_name"}, new String[]{"1234"}));
        assertEquals(Double.valueOf(1), registry.getSampleValue(metricName.safeName(),
                new String[]{"invalid_prometheus_metric_or_label_name"}, new String[]{"1234"}));

        String output = getOutput();
        assertNotNull(output);
    }
}
