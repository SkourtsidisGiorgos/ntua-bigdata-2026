package gr.ntua.dsml.common;

import org.apache.spark.sql.Column;

import static org.apache.spark.sql.functions.*;

/** Great-circle distance, shared by Query 4 (DataFrame column expr and RDD paths). */
public final class Geo {
    private Geo() {}

    static final double EARTH_KM = 6371.0088;

    /** Haversine distance in kilometres between two lat/lon points (degrees). */
    public static double haversineKm(double lat1, double lon1, double lat2, double lon2) {
        double dLat = Math.toRadians(lat2 - lat1);
        double dLon = Math.toRadians(lon2 - lon1);
        double a = Math.sin(dLat / 2) * Math.sin(dLat / 2)
                + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
                * Math.sin(dLon / 2) * Math.sin(dLon / 2);
        return EARTH_KM * 2 * Math.asin(Math.min(1.0, Math.sqrt(a)));
    }

    /**
     * Haversine distance (km) as a Spark Column expression. Stays inside Catalyst
     * codegen (no per-row UDF call), which matters for the Query-4 cross join that
     * evaluates it tens of millions of times.
     */
    public static Column haversineKmCol(Column lat1, Column lon1, Column lat2, Column lon2) {
        Column dLat = radians(lat2.minus(lat1));
        Column dLon = radians(lon2.minus(lon1));
        Column a = pow(sin(dLat.divide(2)), 2)
                .plus(cos(radians(lat1)).multiply(cos(radians(lat2)))
                        .multiply(pow(sin(dLon.divide(2)), 2)));
        Column c = lit(2).multiply(asin(least(lit(1.0), sqrt(a))));
        return c.multiply(EARTH_KM);
    }
}
