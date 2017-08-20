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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author behrouz
 */
public class SparkJavaJob {
    private static class AvgToneCountDualPair implements java.io.Serializable {
      public Double totalAvgTone = 0.0;
      public Integer totalCount = 0;
      public Double windowAvgTone = 0.0;
      public Integer windowCount = 0;

      public AvgToneCountDualPair(final Double totalAvgTone,final Integer totalCount, final Double windowAvgTone, final Integer windowCount) {
        this.totalAvgTone = totalAvgTone;
        this.totalCount = totalCount;
        this.windowAvgTone = windowAvgTone;
        this.windowCount = windowCount;
      }

      public Double getTotalAvgTone() { return totalAvgTone; }
      public void setTotalAvgTone(final Double totalAvgTone) { this.totalAvgTone = totalAvgTone; }
      public Integer getTotalCount() { return totalCount; }
      public void setTotalCount(final Integer totalCount) { this.totalCount = totalCount; }
      public Double getWindowAvgTone() { return windowAvgTone; }
      public void setWindowAvgTone(final Double windowAvgTone) { this.windowAvgTone = windowAvgTone; }
      public Integer getWindowCount() { return windowCount; }
      public void setWindowCount(final Integer windowCount) { this.windowCount = windowCount; }

      @Override
      public String toString() {
        return "(" +
          "totalAvgTone: " + Double.toString(totalAvgTone) +
          ", totalCount: " + Integer.toString(totalCount) +
          ", windowAvgTone: " + Double.toString(windowAvgTone) +
          ", windowCount: " + Integer.toString(windowCount) +
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
        Function3<Tuple2<String, String>, Optional<AvgToneCountDualPair>, State<AvgToneCountDualPair>, Tuple2<Tuple2<String, String>, AvgToneCountDualPair>> stateTransferFunction =
                new Function3<Tuple2<String, String>, Optional<AvgToneCountDualPair>, State<AvgToneCountDualPair>, Tuple2<Tuple2<String, String>, AvgToneCountDualPair>>() {
                    @Override
                    public Tuple2<Tuple2<String, String>, AvgToneCountDualPair> call(Tuple2<String, String> keyPair, Optional<AvgToneCountDualPair> avg, State<AvgToneCountDualPair> state) throws Exception {
                        Double totalTone = avg.orElse(new AvgToneCountDualPair(0.0, 0, 0.0, 0)).getTotalAvgTone() + (state.exists() ? state.get().getTotalAvgTone() : 0.0);
                        Integer totalCount = avg.orElse(new AvgToneCountDualPair(0.0, 0, 0.0, 0)).getTotalCount() + (state.exists() ? state.get().getTotalCount() : 0);

                        AvgToneCountDualPair temp = new AvgToneCountDualPair(totalTone, totalCount, avg.get().getWindowAvgTone(), avg.get().getWindowCount());
                        Tuple2<Tuple2<String, String>, AvgToneCountDualPair> output = new Tuple2<>(keyPair, temp);
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
          .mapToPair(new PairFunction<GDELTEvent, Tuple2<String, String>, AvgToneCountDualPair>() {
              @Override
              public Tuple2<Tuple2<String, String>, AvgToneCountDualPair> call(GDELTEvent gdeltEvent) throws Exception {
                  Pattern r = Pattern.compile("[^/]*://([^/]*)/");
                  Matcher m = r.matcher(gdeltEvent.sourceUrl);

                  String domain = "unknown";
                  if (m.find()) {
                    domain = m.group(1);
                  }

                  return new Tuple2<>(new Tuple2<>(gdeltEvent.actor1Code_countryCode, domain), new AvgToneCountDualPair(gdeltEvent.avgTone, 1, gdeltEvent.avgTone, 1));
              }
          })
          .reduceByKey(new Function2<AvgToneCountDualPair, AvgToneCountDualPair, AvgToneCountDualPair>() {
              @Override
              public AvgToneCountDualPair call(AvgToneCountDualPair first, AvgToneCountDualPair second) throws Exception {
                return new AvgToneCountDualPair(
                  first.getTotalAvgTone() + second.getTotalAvgTone(),
                  first.getTotalCount() + second.getTotalCount(),
                  first.getWindowAvgTone() + second.getWindowAvgTone(),
                  first.getWindowCount() + second.getWindowCount());
              }
          })
          .mapWithState(StateSpec.function (stateTransferFunction))
          .mapToPair(new PairFunction<Tuple2<Tuple2<String, String>, AvgToneCountDualPair>, Tuple2<String, String>, Tuple2<Double, Double>>() {
            @Override
            public Tuple2<Tuple2<String, String>, Tuple2<Double, Double>> call(Tuple2<Tuple2<String, String>, AvgToneCountDualPair> f) throws Exception {
              return new Tuple2<>(f._1, new Tuple2<Double, Double>(f._2.getTotalAvgTone() / f._2.getTotalCount(), f._2.getWindowAvgTone() / f._2.getWindowCount()));
            }
          })
          .map(new Function<Tuple2<Tuple2<String, String>, Tuple2<Double, Double>>, String>() {
              @Override
              public String call(Tuple2<Tuple2<String, String>, Tuple2<Double, Double>> event) throws Exception {
                  return  event._1._1 + "," + event._1._2 + "," + event._2._1 + "," + event._2._2;

              }
          })
          .dstream().saveAsTextFiles(pathToTmp, "");
          //.print();

        jssc.start();
        jssc.awaitTermination();

    }
}
