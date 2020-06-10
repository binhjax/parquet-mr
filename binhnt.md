# Structure
  ## Parquet-Hadoop
     Folder crypto contain encryption function in org.apache.parquet.crypto

# Compile

  1. Compile
    #cd  parquet-hadoop
    #mvn  -Drat.skip=true -Dmaven.test.skip=true -Djapicmp.skip=true  package

    #cd  parquet-teko
    #mvn  -Drat.skip=true -Dmaven.test.skip=true -Djapicmp.skip=true  package

  2. Test access vault

      mvn  -Drat.skip=true -Dmaven.test.skip=true -Djapicmp.skip=true  install
      mvn exec:java
