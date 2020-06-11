# Setup
bash setup.sh
docker-compose up

# Initialize vault
  1. Initialize
     vault operator init -address=http://127.0.0.1:8200

     Unseal Key 1: QSri/PzYqQaTXOhQFIiolsU0Rn0446SeyZrG9IUDj+oZ
     Unseal Key 2: zbT6Cnyv2yN/vq/VGXDmKz2AUNS/mJzsCRwD/BvwyfNk
     Unseal Key 3: zBl9zlUOBb1/gZNRt9xeeqmR92t+GY4/JoNdR/pKRqmK
     Unseal Key 4: Y7TnqByCmJyXXkhHtEnKXt1Ppgx+gghQsy4qc5rLaxZL
     Unseal Key 5: Xz5LR39zgI+M2nWri43KmMbYW4eSBWIVBCwFuvMgtN7O

     Initial Root Token: s.o9Mx60qKPTYIK5hsxKh6XRcZ
  2. Create policy

      vault login  -address=http://127.0.0.1:8200   #Using root token:  s.o9Mx60qKPTYIK5hsxKh6XRcZ

      vault policy write parquet-policy conf/vault/policies/parquet-policy.hcl

  3. Config userpass & create user& password
      vault  auth  enable  -address=http://127.0.0.1:8200 userpass
      vault write  -address=http://127.0.0.1:8200  auth/userpass/users/binhnt \
                      password=123456 \
                      policies=parquet-policy

  4. Test login and policy
      vault login  -address=http://127.0.0.1:8200  -method=userpass \
              username=binhnt \
              password=123456
      vault kv put -address=http://127.0.0.1:8200 kv/parquet/test/123  my-value=s3cr3t
      vault kv get -address=http://127.0.0.1:8200 kv/parquet/test/123
#Initialize minio
   1. Create bucket: encrypted

#Test parquet encryption  
  1. Ecryption data

    docker-compose  exec spark-master /spark/bin/pyspark

    from pyspark.sql import Row

    squaresDF = spark.createDataFrame(
      sc.parallelize(range(1, 6))
       .map(lambda i: Row(int_column=i,  square_int_column=i ** 2)))


    sc._jsc.hadoopConfiguration().set("parquet.crypto.factory.class" , "com.teko.parquet.TekoVaultFactory")
    sc._jsc.hadoopConfiguration().set("encryption.kms.instance.url", "http://vault:8200" )
    sc._jsc.hadoopConfiguration().set("encryption.user.username", "binhnt" )
    sc._jsc.hadoopConfiguration().set("encryption.user.password", "123456" )
    sc._jsc.hadoopConfiguration().set("encryption.secrets.path", "kv/parquet/encrypted/test_encrypted" )
    sc._jsc.hadoopConfiguration().set("encryption.columns", "square_int_column" )


    encryptedParquetPath = "s3a://encrypted/test_encrypted"

    squaresDF.write.parquet(encryptedParquetPath)

  2. Decode parquet encryption
    docker-compose  exec spark-master /spark/bin/pyspark

    sc._jsc.hadoopConfiguration().set("parquet.crypto.factory.class" , "com.teko.parquet.TekoVaultFactory")
    sc._jsc.hadoopConfiguration().set("encryption.kms.instance.url", "http://vault:8200" )
    sc._jsc.hadoopConfiguration().set("encryption.user.username", "binhnt" )
    sc._jsc.hadoopConfiguration().set("encryption.user.password", "123456" )
    sc._jsc.hadoopConfiguration().set("encryption.secrets.path", "kv/parquet/encrypted/test_encrypted")

    encryptedParquetPath = "s3a://encrypted/test_encrypted"
    parquetFile = spark.read.parquet(encryptedParquetPath)
    parquetFile.show()




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
