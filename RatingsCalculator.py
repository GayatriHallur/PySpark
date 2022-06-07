from pyspark.context import SparkContext

sc = SparkContext("local[*]", "RatingsCalculator")

input = sc.textFile("/Users/swaroopmutalik/Desktop/Technical/BigDataCourse/moviedata.data")

mapped_input = input.map(lambda x: (x.split("\t")[2],1))

results = mapped_input.reduceByKey(lambda x, y: (x + y))

sorted_results = results.sortBy(lambda x: x[1], False)

sorted_values = sorted_results.collect()

for result in sorted_values:
    print(result)