**Construir la app**

    ./mvnw package 

**Desplegar el jar en spark** (cargando los datos desde /input)


    ./bin/spark-submit --class org.fuserkuba.DataAnalysis 
    /target/data-analysis-spark-1.0-SNAPSHOT.jar input/
    
    
    
***Ejemplo de salida***

   
    ------------Products (49688 rows) ------------
    root
     |-- product_id: integer (nullable = true)
     |-- product_name: string (nullable = true)
     |-- category: string (nullable = true)
     |-- department: string (nullable = true)
    
    +----------+--------------------+--------------------+---------------+
    |product_id|        product_name|            category|     department|
    +----------+--------------------+--------------------+---------------+
    |         1|Chocolate Sandwic...|       cookies cakes|         snacks|
    |         2|    All-Seasons Salt|   spices seasonings|         pantry|
    |         3|Robust Golden Uns...|                 tea|      beverages|
    |         4|Smart Ones Classi...|        frozen meals|         frozen|
    |         5|Green Chile Anyti...|marinades meat pr...|         pantry|
    |         6|        Dry Nose Oil|    cold flu allergy|  personal care|
    |         7|Pure Coconut Wate...|       juice nectars|      beverages|
    |         8|Cut Russet Potato...|      frozen produce|         frozen|
    |         9|Light Strawberry ...|              yogurt|     dairy eggs|
    |        10|Sparkling Orange ...|water seltzer spa...|      beverages|
    |        11|   Peach Mango Juice|        refrigerated|      beverages|
    |        12|Chocolate Fudge L...|      frozen dessert|         frozen|
    |        13|   Saline Nasal Mist|    cold flu allergy|  personal care|
    |        14|Fresh Scent Dishw...|     dish detergents|      household|
    |        15|Overnight Diapers...|       diapers wipes|         babies|
    |        16|Mint Chocolate Fl...|  ice cream toppings|         snacks|
    |        17|   Rendered Duck Fat|     poultry counter|   meat seafood|
    |        18|Pizza for One Sup...|        frozen pizza|         frozen|
    |        19|Gluten Free Quino...|grains rice dried...|dry goods pasta|
    |        20|Pomegranate Cranb...|       juice nectars|      beverages|
    |        21|Small & Medium De...|       dog food care|           pets|
    |        22|Fresh Breath Oral...|        oral hygiene|  personal care|
    |        23|Organic Turkey Bu...|    packaged poultry|   meat seafood|
    |        24|Tri-Vi-Sol® Vitam...|vitamins supplements|  personal care|
    |        25|Salted Caramel Le...| energy granola bars|         snacks|
    |        26|Fancy Feast Trout...|       cat food care|           pets|
    |        27|Complete Spring W...|   body lotions soap|  personal care|
    |        28|   Wheat Chex Cereal|              cereal|      breakfast|
    |        29|Fresh Cut Golden ...|canned jarred veg...|   canned goods|
    |        30|Three Cheese Ziti...|        frozen meals|         frozen|
    +----------+--------------------+--------------------+---------------+
    only showing top 30 rows
    
    +--------------------+--------------------+
    |            category|products_by_category|
    +--------------------+--------------------+
    |             missing|                1258|
    |     candy chocolate|                1246|
    |       ice cream ice|                1091|
    |vitamins supplements|                1038|
    |              yogurt|                1026|
    +--------------------+--------------------+
    only showing top 5 rows
    
    +-------+--------------------+
    |summary|products_by_category|
    +-------+--------------------+
    |  count|                 135|
    |   mean|  368.05925925925925|
    | stddev|  267.91098329407185|
    |    min|                   1|
    |    25%|                 173|
    |    50%|                 303|
    |    75%|                 499|
    |    max|                1258|
    +-------+--------------------+
    
    +-------------+----------------------+
    |   department|products_by_department|
    +-------------+----------------------+
    |personal care|                  6563|
    |       snacks|                  6264|
    |       pantry|                  5371|
    |    beverages|                  4365|
    |       frozen|                  4007|
    +-------------+----------------------+
    only showing top 5 rows
    
    +-------+----------------------+
    |summary|products_by_department|
    +-------+----------------------+
    |  count|                    22|
    |   mean|    2258.5454545454545|
    | stddev|    1935.0627337504548|
    |    min|                     1|
    |    25%|                  1054|
    |    50%|                  1322|
    |    75%|                  3449|
    |    max|                  6563|
    +-------+----------------------+
    
    +-------+------------------+-----------------+
    |summary|        product_id|        purchases|
    +-------+------------------+-----------------+
    |  count|             49685|            49685|
    |   mean|24844.846855187683|680.6703431619201|
    | stddev|14343.396595224089|4987.769425411178|
    |    min|                 1|                1|
    |    25%|             12427|               18|
    |    50%|             24841|               63|
    |    75%|             37263|              272|
    |    max|             49688|           491291|
    +-------+------------------+-----------------+
    
    ------------Baskets by product (49685 rows) ------------
    root
     |-- product_id: integer (nullable = true)
     |-- purchases: long (nullable = false)
     |-- product_id: integer (nullable = true)
     |-- product_name: string (nullable = true)
     |-- category: string (nullable = true)
     |-- department: string (nullable = true)
    
    +----------+---------+----------+--------------------+--------------------+----------+
    |product_id|purchases|product_id|        product_name|            category|department|
    +----------+---------+----------+--------------------+--------------------+----------+
    |     24852|   491291|     24852|              Banana|        fresh fruits|   produce|
    |     13176|   394930|     13176|Bag of Organic Ba...|        fresh fruits|   produce|
    |     21137|   275577|     21137|Organic Strawberries|        fresh fruits|   produce|
    |     21903|   251705|     21903|Organic Baby Spinach|packaged vegetabl...|   produce|
    |     47209|   220877|     47209|Organic Hass Avocado|        fresh fruits|   produce|
    |     47766|   184224|     47766|     Organic Avocado|        fresh fruits|   produce|
    |     47626|   160792|     47626|         Large Lemon|        fresh fruits|   produce|
    |     16797|   149445|     16797|        Strawberries|        fresh fruits|   produce|
    |     26209|   146660|     26209|               Limes|        fresh fruits|   produce|
    |     27845|   142813|     27845|  Organic Whole Milk|                milk|dairy eggs|
    +----------+---------+----------+--------------------+--------------------+----------+
    only showing top 10 rows
    
    +---------------+-----------------------+
    |     department|purchases_by_department|
    +---------------+-----------------------+
    |        produce|                9888378|
    |     dairy eggs|                5631067|
    |         snacks|                3006412|
    |      beverages|                2804175|
    |         frozen|                2336858|
    |         pantry|                1956819|
    |         bakery|                1225181|
    |   canned goods|                1114857|
    |           deli|                1095540|
    |dry goods pasta|                 905340|
    +---------------+-----------------------+
    only showing top 10 rows
    
    +--------------------+---------------------+
    |            category|purchases_by_category|
    +--------------------+---------------------+
    |        fresh fruits|              3792661|
    |    fresh vegetables|              3568630|
    |packaged vegetabl...|              1843806|
    |              yogurt|              1507583|
    |     packaged cheese|              1021462|
    |                milk|               923659|
    |water seltzer spa...|               878150|
    |      chips pretzels|               753739|
    |     soy lactosefree|               664493|
    |               bread|               608469|
    +--------------------+---------------------+
    only showing top 10 rows
    
    +-------+--------------------+---------------------+
    |summary|            category|purchases_by_category|
    +-------+--------------------+---------------------+
    |  count|                 134|                  135|
    |   mean|                null|    250511.8962962963|
    | stddev|                null|   500943.07738665916|
    |    min|air fresheners ca...|                    3|
    |    25%|                null|                33482|
    |    50%|                null|               104050|
    |    75%|                null|               289488|
    |    max|              yogurt|              3792661|
    +-------+--------------------+---------------------+
    
    
