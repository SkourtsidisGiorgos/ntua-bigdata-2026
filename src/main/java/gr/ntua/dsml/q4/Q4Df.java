package gr.ntua.dsml.q4;

import gr.ntua.dsml.common.Bench;
import gr.ntua.dsml.common.Datasets;
import gr.ntua.dsml.common.Geo;
import gr.ntua.dsml.common.JoinStrategy;
import gr.ntua.dsml.common.Output;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.spark.sql.functions.*;

/**
 * Query 4 - DataFrame API. For each police station: how many crimes are closest
 * to it (vs every other station) and the average distance of those crimes.
 *
 * Join: each crime is matched against all 21 stations (a cross / nearest-neighbour join, which has no equi-key).
 * With the tiny station side this is a broadcast nested-loop join, so the crimes never shuffle for the join;
 * the per-crime argmin is then a group-by on a synthetic crime id.
 *
 * args[0] sets the join hint. Only BROADCAST and SHUFFLE_REPLICATE_NL apply to a non-equi join - MERGE / SHUFFLE_HASH need
 * equi-keys and are ignored by Catalyst here; explain() makes that visible.
 */
public class Q4Df {

    public static Dataset<Row> compute(Dataset<Row> crime, Dataset<Row> stations, String strategy) {
        Dataset<Row> crimes = crime
                .select(col("lat"), col("lon"))
                .filter(col("lat").isNotNull().and(col("lon").isNotNull())
                        .and(col("lat").notEqual(0)).and(col("lon").notEqual(0)))
                .withColumn("crime_id", monotonically_increasing_id());

        Dataset<Row> st = stations.select(
                col("division"),
                col("lat").alias("slat"),
                col("lon").alias("slon"));

        Dataset<Row> dist = crimes
                .crossJoin(JoinStrategy.hint(st, strategy))
                .withColumn("dist", Geo.haversineKmCol(col("lat"), col("lon"), col("slat"), col("slon")));

        // Nearest station per crime: the (dist, division) struct with the smallest dist.
        Dataset<Row> nearest = dist
                .groupBy(col("crime_id"))
                .agg(min(struct(col("dist"), col("division"))).alias("n"))
                .select(col("n.dist").alias("dist"), col("n.division").alias("division"));

        return nearest
                .groupBy(col("division"))
                .agg(count(lit(1)).alias("#"), avg(col("dist")).alias("avg_dist"))
                .select(col("division"),
                        round(col("avg_dist"), 3).alias("average_distance"),
                        col("#"))
                .orderBy(col("#").desc());
    }

    public static void main(String[] args) {
        SparkSession spark = Datasets.session("q4-df");
        String strategy = args.length > 0 ? args[0] : "broadcast";

        Dataset<Row> result = compute(
                Datasets.crime(spark), Datasets.policeStations(spark), strategy);

        System.out.println("== Q4 DataFrame - join strategy hint: " + strategy + " ==");
        result.explain();

        AtomicReference<List<Row>> box = new AtomicReference<>();
        Bench.time("q4-df-" + strategy, () -> box.set(result.collectAsList()));
        Output.show("Q4 (crimes closest to each station)", box.get());

        spark.stop();
    }
}
