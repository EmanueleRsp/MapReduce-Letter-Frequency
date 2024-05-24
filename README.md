# letter-frequency-hadoop-MapReduce

This project is a simple MapReduce program that calculates the frequency of each letter in a text file. The program is written in Java and uses the Hadoop MapReduce framework.

## Usage
1. To run the program, you need to have Hadoop installed on your machine. You can download Hadoop from the [official website](https://hadoop.apache.org/).

2. Clone the repository and navigate to the project directory.

3. Upload the input files in `resources/input` directory.

4. Run the script `run.sh` to compile the Java code, create a JAR file, and run the MapReduce workflow.

5. The output will be saved in the `resources/output` directory.

## Input
The input text file should be uploaded in the `resources/input` directory.

For the input generation, we used [Wikisource](https://it.wikisource.org/wiki/Speciale:Esporta) to download a large number of xml files for a specific category.
Then, we used `script/xml2txt.ipynb` to convert the xml files to txt files (see [here](script/xml2txt.ipynb) for more details).


## Workflow
The program consists of **two jobs**:

### LetterCount
This job reads the input text file and counts the total numeber of letters in the file.
It produces a text file in which the computed value is stored.

Example output:
```
152
```

### LetterFrequency
This job reads the output of the previous job and calculates the frequency of each letter in the text file. It uses the total number of letters to calculate the frequency of each letter, passing it to the reducer as a configuration field.
It produces a text file in which the computed frequencies are stored, lexicographically sorted (if a letter is not present in the text file, it will not be displayed).

Example output:
```
a	0.10526315789473684
b	0.019736842105263157
c	0.06578947368421052
d	0.046052631578947366
e	0.125
f	0.006578947368421052
g	0.019736842105263157
h	0.013157894736842105
i	0.07236842105263158
l	0.07236842105263158
m	0.019736842105263157
n	0.07894736842105263
o	0.09210526315789473
p	0.013157894736842105
q	0.006578947368421052
r	0.07894736842105263
s	0.039473684210526314
t	0.046052631578947366
u	0.046052631578947366
v	0.02631578947368421
z	0.006578947368421052
```