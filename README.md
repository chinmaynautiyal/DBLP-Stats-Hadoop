# CS 441 - Homework 2

## Overview

For this homework we create a map reduce application which processes the DBLP


Consider the following entry in the dataset.
```xml
<inproceedings mdate="2017-05-24" key="conf/icst/GrechanikHB13">
<author>Mark Grechanik</author>
<author>B. M. Mainul Hossain</author>
<author>Ugo Buy</author>
<title>Testing Database-Centric Applications for Causes of Database Deadlocks.</title>
<pages>174-183</pages>
<year>2013</year>
<booktitle>ICST</booktitle>
<ee>https://doi.org/10.1109/ICST.2013.19</ee>
<ee>http://doi.ieeecomputersociety.org/10.1109/ICST.2013.19</ee>
<crossref>conf/icst/2013</crossref>
<url>db/conf/icst/icst2013.html#GrechanikHB13</url>
</inproceedings>
```

This project uses the mapreduce framework to process the dblp dataset, entries from which resemble the sample given above. It computes various statistics based on the given data. 



##Steps to Deploy the application on the Cloudera quickstart VM

1. Run : sbt clean assembly
2. scp the jar using the following command
```scp <jarfile> <username>@<vmIPaddress>:/home/cloudera```
3. Run the command: 

```hadoop jar chinmay_nautiyal_hw2-assembly-0.1.jar dblpStats.driverMapReduce <input-dir> <output-dir>```

(The vm should have the same JDK version: 1.8)
 




## Functionality

This project creates 5 map reduce jobs which perform the following respective computations: 

###1. Counting number of co-authors 

**Output Format:** no of co authors -> no of corresponding entries


**Sample Output:** (the mappings are space separated)

1	13

10	2

2	7

3	6

4	1

###2. Stratifying number of co authors according to given parameters

**Output Format** : no of co authors -> no of corresponding entries

**Sample Output:** (the mappings are space separated)

10	2


###3. Counting authorship score

This job counts the authorship score for a given author, given by the following formula:

The total score for a paper is one. Hence, if an author published 15 papers as the single author without any co-authors, then her score will be 15. For a paper with multiple co-authors the score will be computed using a split weight as the following. First, each co-author receives 1/N score where N is the number of co-authors. Then, the score of the last co-author is credited 1/(4N) leaving it 3N/4 of the original score. The next co-author to the left is debited 1/(4N) and the process repeats until the first author is reached. For example, for a single author the score is one. For two authors, the original score is 0.5. Then, for the last author the score is updated 0.53/4 = 0.5-0.125 = 0.375 and the first author's score is 0.5+0.125 = 0.625. The process repeats the same way for N co-authors.


**Output Format** : author -> calculated co authorship score


**Sample Output:** (the mappings are space separated)

alejandro p. buchmann	0.6666667

benjamin hurwitz	0.375

birgit wesche	1.25

brigitte bartsch-spörl	1.25

burkhard kehrbusch	0.375

cetin ozbutun	0.375

christoph beierle	0.4166667


###4. Min Max Mean and Median number of co authors for each Author

Computes the required statistics based on the number of co authors an author has worked with. 

**Output Format** : author -> min,max,mean,median

**Sample Output:** (the mappings are space separated)

alejandro p. buchmann	3,3,3.0,3.0

benjamin hurwitz	2,2,2.0,2
birgit wesche	1,1,1.0,1
brigitte bartsch-spörl	1,1,1.0,1
burkhard kehrbusch	2,2,2.0,2
cetin ozbutun	2,2,2.0,2
christoph beierle	3,3,3.0,3

###5. Stratified Min Max Mean and Median number of co authors for each Author

Computes the required statistics stratified based on the parameters passed in the configuration file. 


**Output Format** : author -> min,max,mean,median

**Sample Output:** (the mappings are space separated)

alejandro p. buchmann	3,3,3.0,3.0

benjamin hurwitz	2,2,2.0,2
birgit wesche	1,1,1.0,1
brigitte bartsch-spörl	1,1,1.0,1
burkhard kehrbusch	2,2,2.0,2
cetin ozbutun	2,2,2.0,2
christoph beierle	3,3,3.0,3


##AWS EMR

The project was initially tested and deployed on Cloudera's Quickstart VM. This was then moved to AWS Elastic Map Reduce.

[Link to Youtube Video](https://www.youtube.com/watch?v=lVBXWdbHRMQ&feature=youtu.be)





##Outputs

You can find the output of the computations on the dblp.xml file in the output folder of this repository.
