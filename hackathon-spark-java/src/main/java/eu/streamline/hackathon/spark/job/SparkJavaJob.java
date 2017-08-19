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

/**
 * @author behrouz
 */
public class SparkJavaJob {

    public static void main(String[] args) throws InterruptedException {

        ParameterTool params = ParameterTool.fromArgs(args);
        final String pathToGDELT = params.get("path");
        final Long duration = params.getLong("micro-batch-duration", 1000);
        final String country = params.get("country", "USA");

        SparkConf conf = new SparkConf().setAppName("Spark Java GDELT Analyzer");
        String masterURL = conf.get("spark.master", "local[*]");
        conf.setMaster(masterURL);

        JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(duration));
        // checkpoint for storing the state
        jssc.checkpoint("checkpoint/");

        // function to store intermediate values in the state
        // it is called in the mapWithState function of DStream
        Function3<String, Optional<Tuple2<Double, Integer>>, State<Tuple2<Double, Integer>>, Tuple2<String, Tuple2<Double, Integer>>> mappingFunc =
                new Function3<String, Optional<Tuple2<Double, Integer>>, State<Tuple2<Double, Integer>>, Tuple2<String, Tuple2<Double, Integer>>>() {
                    @Override
                    public Tuple2<String, Tuple2<Double, Integer>> call(String country, Optional<Tuple2<Double, Integer>> avg, State<Tuple2<Double, Integer>> state) throws Exception {
                        Double tone = avg.orElse(new Tuple2<Double, Integer>(0.0, 0))._1 + (state.exists() ? state.get()._1 : 0.0);
                        Integer count = avg.orElse(new Tuple2<Double, Integer>(0.0, 0))._2 + (state.exists() ? state.get()._2 : 0);

                        Tuple2<Double, Integer> temp = new Tuple2<Double, Integer>(tone, count);

                        Tuple2<String, Tuple2<Double, Integer>> output = new Tuple2<>(country, temp);
                        state.update(temp);
                        return output;
                    }
        };

        jssc
          .receiverStream(new GDELTInputReceiver(pathToGDELT))
          .filter(new Function<GDELTEvent, Boolean>() {
              @Override
              public Boolean call(GDELTEvent gdeltEvent) throws Exception {
                  return Objects.equals(gdeltEvent.actor1Code_code, country);
              }
          })
          .mapToPair(new PairFunction<GDELTEvent, String, Tuple2<Double, Integer>>() {
              @Override
              public Tuple2<String, Tuple2<Double, Integer>> call(GDELTEvent gdeltEvent) throws Exception {
                  return new Tuple2<>(gdeltEvent.actor1Code_code, new Tuple2<>(gdeltEvent.avgTone, 1));
              }
          })
          .reduceByKey(new Function2<Tuple2<Double, Integer>, Tuple2<Double, Integer>, Tuple2<Double, Integer>>() {
              @Override
              public Tuple2<Double, Integer> call(Tuple2<Double, Integer> first, Tuple2<Double, Integer> second) throws Exception {
                return new Tuple2<>(first._1 + second._1, first._2 + second._2);
              }
          })
          .mapWithState(StateSpec.function (mappingFunc))
          .mapToPair(new PairFunction<Tuple2<String, Tuple2<Double, Integer>>, String, Double>() {
            @Override
            public Tuple2<String, Double> call(Tuple2<String, Tuple2<Double, Integer>> f) throws Exception {
              return new Tuple2(f._1, f._2._1 / f._2._2);
            }
          })
          /*.map(new Function<Tuple2<Date, Double>, String>() {
              @Override
              public String call(Tuple2<Date, Double> event) throws Exception {
                  DateFormat format = new SimpleDateFormat("yyyy-MM-dd");
                  return "Country(" + country + "), Week(" + format.format(event._1()) + "), " +
                          "AvgTone(" + event._2() + ")";

              }
          })*/
          .dstream().saveAsTextFiles("/scratch/dkocher/big-data-summer-school/tmp/data", "txt");

        jssc.start();
        jssc.awaitTermination();

    }
}
