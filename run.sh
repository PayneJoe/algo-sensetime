## wordcount
##/Users/yuanpingzhou/Downloads/spark-2.0.1-bin-hadoop2.7/bin/spark-submit --class wordcount --master local[*] ./target/scala-2.11/sparktest_2.11-1.0.jar /Users/yuanpingzhou/data/spark/sparktest/wordcount/input/README /Users/yuanpingzhou/data/spark/sparktest/wordcount/output /Users/yuanpingzhou/data/spark/sparktest/wordcount/splitoutput

## CTRTest
#/Users/yuanpingzhou/project/spark-2.0.1-bin-hadoop2.7/bin/spark-submit --class CTRTest --master local[*] ./target/scala-2.11/sparktest_2.11-1.0.jar /Users/yuanpingzhou/data/spark/sparktest/ctrtest/input/train_sample /Users/yuanpingzhou/data/spark/sparktest/ctrtest/output

## FMWithSGD
/Users/yuanpingzhou/project/spark-2.0.1-bin-hadoop2.7/bin/spark-submit --class FMWithSGD --master local[*] --driver-library-path /Users/yuanpingzhou/.m2/repository/org/scalanlp/breeze_2.11/0.12 ./target/scala-2.11/sparktest_2.11-1.0.jar /Users/yuanpingzhou/data/spark/sparktest/FMWithSGD/input/a9a /Users/yuanpingzhou/data/spark/sparktest/FMWithSGD/input/a9a.te

