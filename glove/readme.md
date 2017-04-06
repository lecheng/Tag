Glove project is to use Glove to get related words of one word. This project haven't finished yet because of time consumption of running. It should be improved.

test.py is a test code of reading file by python. There are totally four methods to read file by python. It helps to select the best one to save time or space.

sparksplit.py is to select NN,NNP,VB from the Glove corpus and get a file named newvectors.txt, in which every row is a json string.

spark.py is to get the final result. It's also a file in which every row is a json string. The "key" represent the original word. The "value" is a list of key-value, in which the key is the related word and the value is the frequence. It's sorted by the frequence.

You should run the sparksplit.py to get the "newvectors.txt" before running the spark.py.