package Utils;

public final class CommonConstants {

    public static final String POS = "pos";
    public static final String NEG = "neg";
    public static final String POS_LIFT_PATH = "/poslift.csv";
    public static final String NEG_LIFT_PATH = "/neglift.csv";
    public static final String RATINGS_PATH = "/ratings.dat";

    public static final String LIFT_TABLE_NAME = "lift_table";
    public static final String LIFT_TABLE_CF_LIFT = "lift";
    public static final String LIFT_TABLE_CF_LIFT_COL_TOP_K = "top_k";

    public static final String USERS_TABLE_NAME = "users_table";
    public static final String USERS_TABLE_CF_WINDOW = "window";
    public static final String USERS_TABLE_CF_WINDOW_COL_MAX_AGG = "max_agg";
    public static final String USERS_TABLE_CF_WINDOW_COL_PROB_AGG = "prob_agg";

    public static final String USER_ID = "user_id";
    public static final String MOVIE_ID= "movie_id";
    public static final String RATING =  "rating";

    public static final String Q = "Q";

    public static final double MIN_POS_RATING = 3;
    public static final double MAX_NEG_RATING = 2.5;
    public static final int WINDOW_SIZE = 5;


    private CommonConstants(){
        //this prevents even the native class from
        //calling this ctor as well
        throw new AssertionError();
    }
}