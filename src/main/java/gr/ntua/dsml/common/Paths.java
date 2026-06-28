package gr.ntua.dsml.common;

/** HDFS locations of the assignment datasets and output roots. */
public final class Paths {
    private Paths() {}

    public static final String HDFS = "hdfs://hdfs-namenode:9000";
    public static final String DATA = HDFS + "/data";

    public static final String CRIME_2010_2019 = DATA + "/LA_Crime_Data/LA_Crime_Data_2010_2019.csv";
    public static final String CRIME_2020_2025 = DATA + "/LA_Crime_Data/LA_Crime_Data_2020_2025.csv";
    public static final String CENSUS_BLOCKS    = DATA + "/LA_Census_Blocks_2020.geojson";
    public static final String CENSUS_FIELDS    = DATA + "/LA_Census_Blocks_2020_fields.csv";
    public static final String INCOME_2021      = DATA + "/LA_income_2021.csv";
    public static final String POLICE_STATIONS  = DATA + "/LA_Police_Stations.csv";

    /** Per-user writable roots. */
    public static final String USER   = HDFS + "/user/dsml00314";
    public static final String OUTPUT = USER + "/output";
}
