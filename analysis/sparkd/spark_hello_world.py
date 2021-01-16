
import pyspark
sc = pyspark.SparkContext(master='local[6]')

txt = sc.textFile('temp1')
print("Lines in file: {0}\n".format(txt.count()) )

ping_lines = txt.filter(lambda line: '1597945806' in line)
print("Pings sent in 1597945806: {0}\n".format(ping_lines.count()) )
