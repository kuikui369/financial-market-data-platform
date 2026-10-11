package com.quant.platform;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.io.Serializable;
import java.time.Duration;

public class FinancialVwapJob {

    // 1. Data Contract for Ingested Trades
    public static class TradeEvent implements Serializable {
        public long tradeId;
        public String ticker;
        public double price;
        public long volume;
        public long eventTime;

        public TradeEvent() {
        }
    }

    // 2. Incremental O(1) Memory State Accumulator
    public static class VwapAccumulator implements Serializable {
        public double totalVolumeWeightedPrice = 0.0;
        public long totalVolume = 0;
    }

    // 3. Incremental Aggregation: Avoids caching raw trade objects in window memory
    public static class VwapAggregateFunction
            implements AggregateFunction<TradeEvent, VwapAccumulator, VwapAccumulator> {
        @Override
        public VwapAccumulator createAccumulator() {
            return new VwapAccumulator();
        }

        @Override
        public VwapAccumulator add(TradeEvent value, VwapAccumulator acc) {
            acc.totalVolumeWeightedPrice += (value.price * value.volume);
            acc.totalVolume += value.volume;
            return acc;
        }

        @Override
        public VwapAccumulator getResult(VwapAccumulator acc) {
            return acc;
        }

        @Override
        public VwapAccumulator merge(VwapAccumulator a, VwapAccumulator b) {
            a.totalVolumeWeightedPrice += b.totalVolumeWeightedPrice;
            a.totalVolume += b.totalVolume;
            return a;
        }
    }

    // 4. Window Finalizer: Computes final VWAP division upon window trigger
    public static class VwapWindowProcessFunction
            extends ProcessWindowFunction<VwapAccumulator, String, String, TimeWindow> {
        @Override
        public void process(String ticker, Context context, Iterable<VwapAccumulator> elements, Collector<String> out) {
            VwapAccumulator acc = elements.iterator().next();
            double vwap = acc.totalVolume > 0 ? (acc.totalVolumeWeightedPrice / acc.totalVolume) : 0.0;
            out.collect(String.format("WINDOW [%d - %d] | %s | VWAP: $%.2f | Volume: %d",
                    context.window().getStart(), context.window().getEnd(), ticker, vwap, acc.totalVolume));
        }
    }

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // Side Output Tag: Captures delayed trades that arrive past the 5-second
        // watermark
        final OutputTag<TradeEvent> lateTradesTag = new OutputTag<TradeEvent>("late-trades-audit") {
        };

        // Ingest from internal Docker Kafka endpoint (kafka:29092)
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("kafka:29092")
                .setTopics("market-trades")
                .setGroupId("flink-vwap-group")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> rawStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(),
                "KafkaTradeSource");

        // Parse JSON & Assign Bounded-Out-Of-Orderness Watermarks (5s tolerance)
        SingleOutputStreamOperator<TradeEvent> tradeStream = rawStream
                .map(json -> new ObjectMapper().readValue(json, TradeEvent.class))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<TradeEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((event, timestamp) -> event.eventTime));

        // 1-minute window, sliding every 10 seconds, with late trade isolation
        SingleOutputStreamOperator<String> vwapStream = tradeStream
                .keyBy(trade -> trade.ticker)
                .window(SlidingEventTimeWindows.of(Time.minutes(1), Time.seconds(10)))
                .sideOutputLateData(lateTradesTag)
                .aggregate(new VwapAggregateFunction(), new VwapWindowProcessFunction());

        // Emit VWAP results to stdout
        vwapStream.print();

        // Emit late-arriving audit records to stdout
        DataStream<TradeEvent> lateTradesStream = vwapStream.getSideOutput(lateTradesTag);
        lateTradesStream.map(t -> String.format("🚨 LATE AUDIT LOGGED -> Ticker: %s | ID: %d | EventTime: %d",
                t.ticker, t.tradeId, t.eventTime)).print();

        env.execute("Financial-Market-Data-VWAP-Engine");
    }
}