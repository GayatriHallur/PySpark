from pyspark.context import SparkContext

sc = SparkContext("local[*]", "wordcount")

input = sc.textFile("/Users/swaroopmutalik/Desktop/Technical/BigDataCourse/search_data.txt")

mapped_input = input.flatMap(lambda x: x.split(" "))

mapped_lower = mapped_input.map(lambda x: x.lower())

mapped_final = mapped_lower.map(lambda x: (x, 1))

results = mapped_final.reduceByKey(lambda x, y: x + y)

sorted_results = results.sortBy(lambda x: x[1], False)

final_results = sorted_results.collect()

for a in final_results:
    print(a)
