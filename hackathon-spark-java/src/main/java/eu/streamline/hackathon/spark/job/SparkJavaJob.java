package eu.streamline.hackathon.spark.job;

import eu.streamline.hackathon.common.data.GDELTEvent;
import eu.streamline.hackathon.spark.scala.operations.GDELTInputReceiver;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Minutes;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Objects;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * @author behrouz
 */
public class SparkJavaJob {
    private static class AvgToneCountPair implements java.io.Serializable {
      public Double avgTone = 0.0;
      public Integer count = 0;

      public AvgToneCountPair(final Double avgTone,final Integer count) {
        this.avgTone = avgTone;
        this.count = count;
      }

      public Double getAvgTone() { return avgTone; }
      public void setAvgTone(final Double avgTone) { this.avgTone = avgTone; }
      public Integer getCount() { return count; }
      public void setCount(final Integer count) { this.count = count; }

      @Override
      public String toString() {
        return "(" +
          "avgTone: " + Double.toString(avgTone) +
          ", count: " + Integer.toString(count) +
          ")";
      }
    }

    public static void main(String[] args) throws InterruptedException {

        ParameterTool params = ParameterTool.fromArgs(args);
        final String pathToGDELT = params.get("path");
        final String pathToTmp = params.getRequired("tmpdir");
        final Long duration = params.getLong("micro-batch-duration", 1000);
        final String countries_str = params.get("countries", "USA RUS DEU AUT ESP CHN FRA");

        final List<String> countries = Arrays.asList(countries_str.split("\\s+"));

        SparkConf conf = new SparkConf().setAppName("Spark Java GDELT Analyzer");
        String masterURL = conf.get("spark.master", "local[*]");
        conf.setMaster(masterURL);

        JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(duration));
        // checkpoint for storing the state
        jssc.checkpoint("checkpoint/");

        // function to store intermediate values in the state
        // it is called in the mapWithState function of DStream
        Function3<String, Optional<AvgToneCountPair>, State<AvgToneCountPair>, Tuple2<String, AvgToneCountPair>> mappingFunc =
                new Function3<String, Optional<AvgToneCountPair>, State<AvgToneCountPair>, Tuple2<String, AvgToneCountPair>>() {
                    @Override
                    public Tuple2<String, AvgToneCountPair> call(String country, Optional<AvgToneCountPair> avg, State<AvgToneCountPair> state) throws Exception {
                        Double tone = avg.orElse(new AvgToneCountPair(0.0, 0)).getAvgTone() + (state.exists() ? state.get().getAvgTone() : 0.0);
                        Integer count = avg.orElse(new AvgToneCountPair(0.0, 0)).getCount() + (state.exists() ? state.get().getCount() : 0);

                        AvgToneCountPair temp = new AvgToneCountPair(tone, count);
                        Tuple2<String, AvgToneCountPair> output = new Tuple2<>(country, temp);
                        state.update(temp);

                        return output;
                    }
        };

        jssc
          .receiverStream(new GDELTInputReceiver(pathToGDELT))
          .filter(new Function<GDELTEvent, Boolean>() {
              @Override
              public Boolean call(GDELTEvent gdeltEvent) throws Exception {
                  return countries.contains(gdeltEvent.actor1Code_code);
              }
          })
          .mapToPair(new PairFunction<GDELTEvent, String, AvgToneCountPair>() {
              @Override
              public Tuple2<String, AvgToneCountPair> call(GDELTEvent gdeltEvent) throws Exception {
                  return new Tuple2<>(gdeltEvent.actor1Code_countryCode, new AvgToneCountPair(gdeltEvent.avgTone, 1));
              }
          })
          .reduceByKey(new Function2<AvgToneCountPair, AvgToneCountPair, AvgToneCountPair>() {
              @Override
              public AvgToneCountPair call(AvgToneCountPair first, AvgToneCountPair second) throws Exception {
                return new AvgToneCountPair(first.getAvgTone() + second.getAvgTone(), first.getCount() + second.getCount());
              }
          })
          .mapWithState(StateSpec.function (mappingFunc))
          .mapToPair(new PairFunction<Tuple2<String, AvgToneCountPair>, String, Double>() {
            @Override
            public Tuple2<String, Double> call(Tuple2<String, AvgToneCountPair> f) throws Exception {
              return new Tuple2<>(f._1, f._2.getAvgTone() / f._2.getCount());
            }
          })
          .map(new Function<Tuple2<String, Double>, String>() {
              @Override
              public String call(Tuple2<String, Double> event) throws Exception {
                  return  event._1 + "," + event._2;

              }
          })
          .dstream().saveAsTextFiles(pathToTmp, "");
          //.print();

        jssc.start();
        jssc.awaitTermination();

    }
}
