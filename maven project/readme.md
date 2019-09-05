# Map Reduce Samples

This is my repo containing some map reduce code samples. Each same is in a different Java package namespace.

For each sample, I have included some simple tests using mrunit (map-reduce unit).

It is a standard Maven project, so you can build it with:

    mvn clean install

## WordCount

package: com.amal.Hadoop 

This is a variation on the wordcount example, where I take a CSV file and spilt each line on comma in the mapper.

Then I pass the result to a reducer where it computes the total occurrences of each unique word in the input files.


## Fan Out

package: com.amal.Hadoop.Fan

The idea of this example is to take an input file, and fan-out the records into many output files. This example uses only a mapper with no reducer.

It reads the input file and extracts the first character of each line. The line is then written into a file with the same name as the character.

In other words each line is bucketed into a set of output files. This example isn't very useful on its own, but it illustrates how to write to many different output files.
