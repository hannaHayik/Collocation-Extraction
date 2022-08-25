# Collocation-Extraction
Application that automatically extracts collocations from the Google 2-grams dataset using Amazon Elastic Map Reduce  
   
Stack: AWS Java SDK, Hadoop, EMR, S3  
  
A collocation is a sequence of words that co-occur more often than would be expected by chance. The identification of collocations - such as 'crystal clear', 'cosmetic surgery' - is essential for many natural language processing and information extraction applications.  
In this application, we will use **log likelihood ratio** in order to determine whether a given pair of ordered words is a collocation.  
  
  
**log likelihood ratio** Page 22, Equations (5.10) https://nlp.stanford.edu/fsnlp/promo/colloc.pdf  
  
    
Map-Reduce program which produces the list of top-100 collocations for each decade (1990-1999, 2000-2009, etc.) for English and Hebrew, with their log likelihood ratios (in descending order).  
  
Input: Google Bigrams  
▪ English: s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-us-all/2gram/data  
▪ Hebrew: s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/2gram/data  
  
  
Algorithm:  
We are looking for 4 parameters to complete the equation from (5.10):  
N: all words from the corpus, we count them when inputting the bigrams in the first step Map  
c1: number of occurences of every 1st word in a bigram  
c2: number of occurences of every 2nd word in a bigram  
c1c2: number of occurences of the bigram itself (given in the input for every bigram)  
  
for every bigram we add two keys: <w1, *asterisk*> and <w1,w2>  
and depending on Hadoop sort & reduce we should get the count of the 1st word for every bigram in the following manner:  
<w1, astersik>  
<w1, w2>  
<w1, w3>  
<w1, w4> ... 

doing the same for the 2nd word in Step2 would finally result in finding all the needed variables to calculate the Log likelihood ratio for every bigram.  
