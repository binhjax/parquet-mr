# Setup
cd spark-test

bash setup.sh
docker-compose up

# Initialize vault
  1. Initialize
     + Initlize vault:
       vault operator init -address=http://127.0.0.1:8200

       Unseal Key 1: yJko86Ar5mPh+NrkXEiGooPRTReUOxjHPZBkBd48Euyy
       Unseal Key 2: 9MVnzChDZg2dNyvYhs8LgEVaLXMT00dNGS6DcD4f3GPe
       Unseal Key 3: LRrng1RqLk34bJdcmSBS5S6rhkcjH5MeXl19SEOHvOOh
       Unseal Key 4: 1flfdyqrhBarIAjRIAuZFCDXiCYpYYBBG/KyxexziZx1
       Unseal Key 5: LBDn2Pmf5VCbMZZfCo18ay7Eci5d3qupAkcg40pOcJYi

       Initial Root Token: s.gM7QlBHNsRK0I3r7ohw8aYrg

    + Unseal:
      Repeat below command in 3 times:

      # vault operator unseal  -address=http://127.0.0.1:8200

      Enter 3 above keys:
      Unseal Key 1: yJko86Ar5mPh+NrkXEiGooPRTReUOxjHPZBkBd48Euyy
      Unseal Key 2: 9MVnzChDZg2dNyvYhs8LgEVaLXMT00dNGS6DcD4f3GPe
      Unseal Key 3: LRrng1RqLk34bJdcmSBS5S6rhkcjH5MeXl19SEOHvOOh

  2. Create policy

      vault login  -address=http://127.0.0.1:8200   
      #Using root token:  s.gM7QlBHNsRK0I3r7ohw8aYrg

      vault policy    write -address=http://127.0.0.1:8200  parquet-policy conf/vault/policies/parquet-policy.hcl

  3. Config userpass & create user& password
      vault  auth  enable  -address=http://127.0.0.1:8200 userpass
      vault write  -address=http://127.0.0.1:8200  auth/userpass/users/binhnt \
                      password=123456 \
                      policies=parquet-policy

  5. Create serets engine:
      vault secrets enable -version=2  -address=http://127.0.0.1:8200 kv
  6. Test login and policy
      vault login  -address=http://127.0.0.1:8200  -method=userpass \
              username=binhnt \
              password=123456
      vault kv put -address=http://127.0.0.1:8200 kv/parquet/test/123  my-value=s3cr3t
      vault kv get -address=http://127.0.0.1:8200 kv/parquet/test/123

#Initialize minio
   1. Create bucket: encrypted
   http://localhost:9900/minio/

#Test parquet encryption  
  1. Ecryption data

docker-compose  exec spark-master /spark/bin/pyspark

from pyspark.sql import Row
squaresDF = spark.createDataFrame(sc.parallelize(range(1, 6)).map(lambda i: Row(int_column=i,  square_int_column=i ** 2)))


sc._jsc.hadoopConfiguration().set("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint","http://storage:9000")
sc._jsc.hadoopConfiguration().set("fs.s3a.access.key","minioadmin")
sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key","minioadmin")
sc._jsc.hadoopConfiguration().set("fs.s3a.path.style.access","True")

sc._jvm.com.teko.parquet.TekoVaultFactory.InitializeKeys(sc._jsc.hadoopConfiguration())

no_encryptedParquetPath = "s3a://encrypted/no_encrypted"
squaresDF.write.parquet(no_encryptedParquetPath)


sc._jsc.hadoopConfiguration().set("parquet.crypto.factory.class" , "com.teko.parquet.TekoVaultFactory")
sc._jsc.hadoopConfiguration().set("encryption.kms.instance.url", "http://vault:8200" )
sc._jsc.hadoopConfiguration().set("encryption.user.username", "binhnt" )
sc._jsc.hadoopConfiguration().set("encryption.user.password", "123456" )
sc._jsc.hadoopConfiguration().set("encryption.secrets.path", "kv/parquet/encrypted/test_encrypted" )
sc._jsc.hadoopConfiguration().set("encryption.columns", "square_int_column" )

sc._jvm.com.teko.parquet.TekoVaultFactory.InitializeKeys(sc._jsc.hadoopConfiguration())

#java_import(sparkContext._jvm, "com.teko.parquet.TekoVaultFactory")
#TekoVaultFactory  = spark.sparkContext._jvm.TekoVaultFactory()
#TekoVaultFactory.createColumnKeys(sc._jsc.hadoopConfiguration())

encryptedParquetPath = "s3a://encrypted/test_encrypted"
squaresDF.write.parquet(encryptedParquetPath)

  2. Decode parquet encryption



docker-compose  exec spark-master /spark/bin/pyspark

sc._jsc.hadoopConfiguration().set("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint","http://storage:9000")
sc._jsc.hadoopConfiguration().set("fs.s3a.access.key","minioadmin")
sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key","minioadmin")
sc._jsc.hadoopConfiguration().set("fs.s3a.path.style.access","True")


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



      -Dmaven.test.skip=true -Dlicense.skip=true  -Drat.skip=true  package
