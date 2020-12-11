package org.fuserkuba;

import org.apache.commons.lang.ArrayUtils;
import org.apache.log4j.Logger;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.evaluation.ClusteringEvaluator;
import org.apache.spark.ml.feature.QuantileDiscretizer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.fpm.FPGrowth;
import org.apache.spark.ml.fpm.FPGrowthModel;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.tuning.CrossValidator;
import org.apache.spark.ml.tuning.CrossValidatorModel;
import org.apache.spark.ml.tuning.ParamGridBuilder;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Map;

import static org.apache.spark.sql.functions.*;

public class DataAnalysis {


    private static SparkSession spark;

    private static String ORDERS = "orders.csv";
    private static String AISLES = "aisles.csv";
    private static String PRODUCTS = "products.csv";
    private static String DEPARTMENTS = "departments.csv";
    private static String ORDERS_PRODUCTS_TRAIN = "order_products__train.csv";
    private static String ORDERS_PRODUCTS_PRIOR = "order_products__prior.csv";

    private static Dataset<Row> dsClients;
    private static Dataset<Row> dsBaskets;
    private static Dataset<Row> dsProducts;

    private static Logger logger = Logger.getLogger(DataAnalysis.class);

    public static void main(String[] args) {


        if (args.length < 1) {
            System.err.println("Usage: DataAnalysis <file>");
            System.exit(1);
        }

        spark = SparkSession.builder().appName("DataAnalysis").getOrCreate();

        //only error logs
        spark.sparkContext().setLogLevel("ERROR");
        //directory
        Calendar time = GregorianCalendar.getInstance();

        String directory = args[0].concat("/");
        loadDataSet(directory);

        System.out.println("------------".concat((GregorianCalendar.getInstance().getTimeInMillis() - time.getTimeInMillis()) / 1000 / 60 + " minuts elapsed"));
        if (dsClients == null || dsBaskets == null || dsProducts == null) {
            //Extract and Transform
            extract_transform_dataSet(directory);
            System.out.println("------------".concat((GregorianCalendar.getInstance().getTimeInMillis() - time.getTimeInMillis()) / 1000 / 60 + " minuts elapsed"));
            //Load
            saveDataSet(directory);
            System.out.println("------------".concat((GregorianCalendar.getInstance().getTimeInMillis() - time.getTimeInMillis()) / 1000 / 60 + " minuts elapsed"));
        }
        //Exploratory Analysis
        //exploratoryAnalysis();
        //Cluster Analysis
        Dataset<Row> dsSegments = segmentClientsByRFM(directory, new int[]{10, 9, 8, 7, 6, 5, 4, 3});
        System.out.println("------------".concat((GregorianCalendar.getInstance().getTimeInMillis() - time.getTimeInMillis()) / 1000 / 60 + " minuts elapsed"));
        //Market Basket Analysis
        marketBasketAnalysis(dsSegments);
        //
        System.out.println("------------".concat((GregorianCalendar.getInstance().getTimeInMillis() - time.getTimeInMillis()) / 1000 / 60 + " minuts elapsed"));
    }

    private static void extract_transform_dataSet(String dataPath) {
        Dataset<Row> dsOrders = readDataSet(dataPath.concat(ORDERS));
        Dataset<Row> dsOrdersProductsTrain = readDataSet(dataPath.concat(ORDERS_PRODUCTS_TRAIN));
        Dataset<Row> dsOrdersProductsPrior = readDataSet(dataPath.concat(ORDERS_PRODUCTS_PRIOR));
        dsProducts = readDataSet(dataPath.concat(PRODUCTS));
        Dataset<Row> dsAisles = readDataSet(dataPath.concat(AISLES));
        Dataset<Row> dsDepartments = readDataSet(dataPath.concat(DEPARTMENTS));

        System.out.println("------------".concat("Datasets extracted from ").concat(dataPath).concat("  Starting transformation..."));
        // OrdersProducts
        Dataset<Row> dsOrdersProducts = dsOrdersProductsPrior.union(dsOrdersProductsTrain).withColumnRenamed("order_id", "order");
        // Baskets
        dsBaskets = dsOrdersProducts.join(dsOrders, dsOrdersProducts.col("order").equalTo(dsOrders.col("order_id")), "left")
                .select("order", "product_id", "user_id", "order_hour_of_day", "order_dow");
        dsBaskets = dsBaskets.withColumnRenamed("order", "basket_id")
                .withColumnRenamed("user_id", "client_id")
                .withColumnRenamed("order_hour_of_day", "purchase_hour")
                .withColumnRenamed("order_dow", "purchase_day");
        //Recency: Days prior order
        Dataset<Row> dsUsersLastDays = dsOrders.na().drop().groupBy("user_id").agg(avg("days_since_prior_order").as("avg_last_days"));
        //Monetary: Products
        Dataset<Row> dsUsersProducts = dsBaskets.groupBy("client_id").count().as("products");
        //Dataset<Row> dsUsersHours = dsOrders.groupBy("user_id").agg(avg("order_hour_of_day").as("avg_hour")).sort("user_id");

        Dataset<Row> dsUsersHoursMode = dsBaskets.groupBy("client_id", "purchase_hour").agg(countDistinct("basket_id").as("basket_by_hour"),
                count("product_id").as("product_by_hour")).
                sort(col("client_id"), col("basket_by_hour").desc());
        //obtener la hora "típica" de compra, se toma la hora con más compras y más productos comprados
        WindowSpec spec = Window.partitionBy("client_id").orderBy(col("basket_by_hour").desc(), col("product_by_hour").desc());
        dsUsersHoursMode = dsUsersHoursMode.withColumn("dense_rank", dense_rank().over(spec)).sort("client_id", "dense_rank")
                .where(col("dense_rank").equalTo(1))
                .groupBy("client_id").agg(first("purchase_hour").as("typical_hour")).sort("client_id");
        //compras por usuario por día de la semana
        Dataset<Row> dsUsersDow = dsBaskets.stat().crosstab("client_id", "purchase_day").sort("client_id_purchase_day")
                .withColumnRenamed("client_id_purchase_day", "user_id")
                .withColumnRenamed("0", "saturday")
                .withColumnRenamed("1", "sunday")
                .withColumnRenamed("2", "monday")
                .withColumnRenamed("3", "tuesday")
                .withColumnRenamed("4", "wednesday")
                .withColumnRenamed("5", "thursday")
                .withColumnRenamed("6", "friday");
        //Users & LastDays & Orders
        dsClients = dsUsersLastDays.withColumnRenamed("user_id", "user")
                .join(dsOrders.groupBy("user_id").count().withColumnRenamed("count", "orders"), col("user").equalTo(col("user_id")), "left")
                .drop("client_id");
        //Users & LastDays & Orders & Products
        dsClients = dsClients.join(dsUsersProducts.withColumnRenamed("count", "products"), col("user").equalTo(dsUsersProducts.col("client_id")), "left");
        //Users & LastDays & Orders & Products & Week
        dsClients = dsClients.join(dsUsersDow, col("user").equalTo(dsUsersDow.col("user_id")), "left")
                .drop("user_id").drop("client_id");
        //Users & LastDays & Orders & Products & Week & Hours
        dsClients = dsClients.join(dsUsersHoursMode, col("user").equalTo(dsUsersHoursMode.col("client_id")), "left")
                .withColumnRenamed("user", "client").drop("client_id");
        //Products
        dsProducts = dsProducts.join(dsAisles, dsProducts.col("aisle_id").equalTo(dsAisles.col("aisle_id")), "left")
                .join(dsDepartments, dsProducts.col("department_id").equalTo(dsDepartments.col("department_id")), "left")
                .drop("aisle_id").drop("department_id").withColumnRenamed("aisle", "category");
        System.out.println("------------".concat("Datasets transformed from ").concat(dataPath).concat("  Starting load..."));
    }

    private static void saveDataSet(String directory) {
        dsBaskets.write().mode(SaveMode.Overwrite).option("sep", ",").option("header", true).csv(directory.concat("dsBaskets.csv"));
        System.out.println("------------".concat("Baskets dataset (").concat(dsBaskets.count() + " rows). Saved ------------"));

        dsClients.write().mode(SaveMode.Overwrite).option("sep", ",").option("header", true).csv(directory.concat("dsClients.csv"));
        System.out.println("------------".concat("Clients dataset (").concat(dsClients.count() + " rows). Saved ------------"));

        dsProducts.write().mode(SaveMode.Overwrite).option("sep", ",").option("header", true).csv(directory.concat("dsProducts.csv"));
        System.out.println("------------".concat("Products dataset (").concat(dsProducts.count() + " rows). Saved ------------"));
    }

    private static void loadDataSet(String directory) {
        try {
            dsBaskets = readDataSet(directory.concat("dsBaskets.csv"));
            System.out.println("------------".concat("Baskets dataset (").concat(dsBaskets.count() + " rows). Loaded ------------"));

            dsClients = readDataSet(directory.concat("dsClients.csv"));
            System.out.println("------------".concat("Clients dataset (").concat(dsClients.count() + " rows). Loaded ------------"));

            dsProducts = readDataSet(directory.concat("dsProducts.csv"));
            System.out.println("------------".concat("Products dataset (").concat(dsProducts.count() + " rows). Loaded ------------"));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void exploratoryAnalysis() {
        System.out.println("------------".concat("Exploratory analysis starting..."));
        //dsBaskets exploration
        describeDataSet(dsBaskets, "Baskets", 10);
        Dataset<Row> baskets = dsBaskets.groupBy("basket_id").agg(count("product_id").as("products"), first("client_id").as("client_id"), first("purchase_hour").as("hour"), first("purchase_day").as("day"));
        baskets.printSchema();
        baskets.select(col("products").as("products_by_basket")).summary().show();
        baskets.groupBy("hour").agg(count("products").as("products")).orderBy(col("products").desc()).show(24);
        baskets.groupBy("day").agg(count("products").as("products")).orderBy(col("products").desc()).show();
        baskets.groupBy("client_id").agg(sum("products").as("products_by_client")).orderBy(col("products_by_client")).summary().show();
        //dsClients exploration
        describeDataSet(dsClients.sort("client"), "Clients", 10);
        dsClients.select("products", "avg_last_days", "typical_hour").summary().show();
        dsClients.select("orders", "saturday", "sunday", "monday", "tuesday", "wednesday", "thursday", "friday").summary().show();

        //dsProducts exploration
        describeDataSet(dsProducts, "Products", 30);
        dsProducts.groupBy("category").agg(count("product_id").as("products_by_category")).orderBy(col("products_by_category").desc()).show(5);
        dsProducts.groupBy("category").agg(count("product_id").as("products_by_category")).select("products_by_category").summary().show();
        dsProducts.groupBy("department").agg(count("product_id").as("products_by_department")).orderBy(col("products_by_department").desc()).show(5);
        dsProducts.groupBy("department").agg(count("product_id").as("products_by_department")).select("products_by_department").summary().show();
        //Purchases by product
        Dataset<Row> purchases = dsBaskets.groupBy("product_id").count().withColumnRenamed("count", "purchases");
        purchases.summary().show();
        purchases = purchases.join(dsProducts, purchases.col("product_id").equalTo(dsProducts.col("product_id")), "left");
        describeDataSet(purchases.orderBy(col("purchases").desc()), "Baskets by product", 10);
        purchases.groupBy("department").agg(sum("purchases").as("purchases_by_department")).sort(col("purchases_by_department").desc()).show(10);
        //Purchases by category
        purchases = purchases.groupBy("category").agg(sum("purchases").as("purchases_by_category")).sort(col("purchases_by_category").desc());
        purchases.show(10);
        purchases.summary().show();
    }

    private static void describeDataSet(Dataset dataset, String title, int rows) {
        //display schema of data
        System.out.println("------------".concat(title + " (").concat(dataset.count() + " rows) ------------"));
        dataset.printSchema();
        dataset.show(rows);
    }

    private static Dataset<Row> readDataSet(String dataPath) {
        return spark.read()
                .option("sep", ",")
                .option("header", true)
                .option("inferSchema", true)
                .csv(dataPath).toDF();

    }

    /**
     * @param directory Directory to save clients segmentation data set
     * @param k_values  List of possible number of clusters for clients segmentation
     */
    private static Dataset<Row> segmentClientsByRFM(String directory, int[] k_values) {
        //Using pipeline and CrossValidator
        Dataset<Row> clientsRFM = dsClients.select("client", "avg_last_days", "orders", "products");

        String[] features = new String[]{"recency", "frequency", "monetary"};
        //Features transformation
        QuantileDiscretizer discretizer = new QuantileDiscretizer()
                .setInputCols(new String[]{"avg_last_days", "orders", "products"})
                .setOutputCols(features)
                .setNumBucketsArray(new int[]{10, 10, 10});

        //Select Features
        VectorAssembler assembler = new VectorAssembler()
                .setInputCols(features)
                .setOutputCol("features");

        //Set Kmeans
        KMeans kMeans = new KMeans().setSeed(1L);
        //Prepare pipeline
        Pipeline pipeline = new Pipeline()
                .setStages(new PipelineStage[]{discretizer, assembler, kMeans});

        // Evaluate clustering by computing Silhouette score
        ClusteringEvaluator evaluator = new ClusteringEvaluator();

        ParamMap[] paramGrid = new ParamGridBuilder()
                .addGrid(kMeans.k(), k_values)
                .build();
        //CrossValidator
        CrossValidator cv = new CrossValidator()
                .setEstimator(pipeline)
                .setEvaluator(new ClusteringEvaluator())
                .setEstimatorParamMaps(paramGrid)
                .setNumFolds(2)  // Use 3+ in practice
                .setParallelism(2);  // Evaluate up to 2 parameter settings in parallel

        // Run cross-validation, and choose the best set of parameters.
        CrossValidatorModel cvModel = cv.fit(clientsRFM);
        Dataset<Row> crossValidatorPredictions = cvModel.transform(clientsRFM);
        //Obtain bestModel
        KMeansModel bestKMeans = (KMeansModel) ((PipelineModel) cvModel.bestModel()).stages()[2];
        //Explain
        System.out.println("Parameters: " + cvModel.explainParams());
        System.out.println("clusters: " + bestKMeans.getK());
        //Centers
        Vector[] centers = bestKMeans.clusterCenters();
        System.out.println("Cluster Centers: ".concat(ArrayUtils.toString(features)));
        for (Vector center : centers) {
            System.out.println(center);
        }
        System.out.println("CrossValidator Clustered data");
        crossValidatorPredictions.printSchema();
        crossValidatorPredictions.groupBy("prediction").count().orderBy(desc("count")).show();

        double silhouette = evaluator.evaluate(crossValidatorPredictions);
        System.out.println("CrossValidator: Silhouette with squared euclidean distance = " + silhouette);
        crossValidatorPredictions.drop("features").repartition(1)
                .write().mode(SaveMode.Overwrite)
                .option("sep", ",").option("header", true)
                .csv(directory.concat("predictions.csv"));
        System.out.println("------------".concat("predictions dataset (").concat(crossValidatorPredictions.count() + " rows). Saved ------------"));

        //Describe segments
        for (int i = 0; i < bestKMeans.getK(); i++) {
            System.out.println("------------".concat("Cluster (" + i + ") ------------"));
            crossValidatorPredictions.drop("features")
                    .filter(col("prediction").equalTo(i))
                    .summary().show();
        }

        return crossValidatorPredictions.withColumnRenamed("prediction", "segment");
    }

    private static void marketBasketAnalysis(Dataset<Row> dsClientsSegmented) {
        //Join baskets with segments
        Dataset<Row> basketItems = dsBaskets.join(dsClientsSegmented,
                dsBaskets.col("client_id").equalTo(dsClientsSegmented.col("client")),
                "inner")
                .join(dsProducts, dsBaskets.col("product_id").equalTo(dsProducts.col("product_id").as("product")),
                        "left")
                .groupBy("basket_id")
                .agg(collect_set("product_name").as("items"),
                        count(dsBaskets.col("product_id")).as("k"),
                        first("client_id").as("client_id"),
                        first("segment").as("segment"));

        describeDataSet(basketItems, "Baskets items for FPM", 5);


        //Parameters for segments: [segment]->{min_support,min_confidence}
        Map<Integer, Double[]> segmentsParameters = new HashMap<Integer, Double[]>();
        //segmentsParameters.put(0,new Double[]{0.01,0.15});
        segmentsParameters.put(1, new Double[]{0.01, 0.15});
        //segmentsParameters.put(2,new Double[]{0.01,0.15});
        //segmentsParameters.put(3,new Double[]marketBasketAnalysis{0.01,0.15});

        for (Integer segment : segmentsParameters.keySet()) {
            System.out.println("------------Market basket analysis for segment " + segment);
            Dataset<Row> basketItems4Segment = basketItems.filter(col("segment").equalTo(segment))
                    .sort(desc("k"));
            //describeDataSet(basketItems4Segment,"BasketsItems for segment "+segment,10);
            basketItems4Segment.summary("count").show();
            findRules(basketItems4Segment, segmentsParameters.get(segment)[0], segmentsParameters.get(segment)[1]);
        }
    }

    private static Dataset<Row> findRules(Dataset<Row> basketItems, double support, double confidence) {

        System.out.println("------------FPM Analysis started with min_support: " + support + " and min_confidence: " + confidence);

        FPGrowthModel model = new FPGrowth()
                .setItemsCol("items")
                .setMinSupport(support)
                .setMinConfidence(confidence)
                .fit(basketItems);

        // Display frequent itemsets.
        model.freqItemsets().sort(desc("freq")).show();
        // Display generated association rules.
        Dataset<Row> dsRules = model.associationRules().sort(desc("confidence"));
        dsRules.show();
        return dsRules;
        // transform examines the input items against all the association rules and summarize the
        // consequents as prediction
        //model.transform(itemsDF).show();
    }
}
