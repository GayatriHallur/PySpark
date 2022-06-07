from pyspark.context import SparkContext

sc = SparkContext("local[*]", "RatingsCalculator")

input = sc.textFile("/Users/swaroopmutalik/Desktop/Technical/BigDataCourse/customerorders.csv")

mapped_input = input.map(lambda x: (x.split(",")[0], float(x.split(",")[2])))

total_customer = mapped_input.reduceByKey(lambda x, y: (x + y))

sorted_data = total_customer.sortBy(lambda x: x[1], False)

sorted_results = sorted_data.collect()

for result in sorted_results:
    print(result)
