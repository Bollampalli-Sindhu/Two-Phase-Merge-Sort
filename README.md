# Two-Phase-Merge-Sort
Implementation of the two-phase merge sort algorithm to sort a large number of records. The metadata file will contain information about the size of the
different columns (in bytes). The data type for all columns are assumed to be a string. The number of columns can range from 1 to 20. The program is capable of sorting in both ascending and descending order. Different values of main memory (M) usage allowed and also different size of files. The implementation has two parts:
  
  - Implementation without using threads.
  - First phase of two-phase merge sort is parallelised using threads. Number of threads (T) to be used in the program is provided as command line argument.
### Execution
- Without using threads
  > ./sort input.txt output.txt M asc(or desc) col1 col2
- Using threads
  > ./sort input.txt output.txt M T desc(or asc) col1 col2
### Assumptions
- If an empty line is encountered while reading the input file, it is considered as end of the file 
