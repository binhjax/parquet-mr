package com.teko;

import com.bettercloud.vault.SslConfig;
import com.bettercloud.vault.VaultConfig;
import com.bettercloud.vault.Vault;
import com.bettercloud.vault.VaultException;

import com.bettercloud.vault.response.LogicalResponse;
import com.bettercloud.vault.response.AuthResponse;

import java.util.HashMap;
import java.util.Map;
import java.util.Hashtable;

import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.Base64;

public class HitachiVaultClient {

  public static void main(String args[]) {
    Random random = ThreadLocalRandom.current();
    byte[] randomBytes = new byte[16];
    random.nextBytes(randomBytes);
    String encoded = Base64.getUrlEncoder().encodeToString(randomBytes);

     System.out.println("Key length: " + encoded.length());

     // String vault_url = "http://127.0.0.1:8200";
     // String access_token = "s.o9Mx60qKPTYIK5hsxKh6XRcZ";
     // String secrets_path = "kv/parquet/abc/123";
     //
     //
     // // Map<String, String> secretEngineVersions = new HashMap<String, String>();
     // // secretEngineVersions.put(secrets_path,"1");
     //
     // System.out.println("1. Create vault config ");
     // try {
     //   VaultConfig config = new VaultConfig()
     //                                // .engineVersion(1)
     //                                .address(vault_url)
     //                                .token(access_token)
     //                                // .secretsEnginePathMap(secretEngineVersions)
     //                                .sslConfig(new SslConfig().verify(false).build())
     //                                .build();
     //
     //   Vault vault = new Vault(config,2);
     //
     //   //1. Login with AppRole
     //   // String role_id = "42fd8e84-1505-09af-5256-7bad7725b27c";
     //   // String secret_id = "30bc27cf-d6ac-5459-5f4f-2c67a2be622b";
     //   // AuthResponse response = vault.auth().loginByAppRole(role_id, secret_id);
     //
     //   //2. Login with Userpass
     //   String username = "binhnt";
     //   String password = "123456";
     //   AuthResponse response = vault.auth().loginByUserPass(username, password);
     //
     //   //3. Login with wrappingToken: failed
     //   // String wrappingToken = "s.6m8iBvHLNUCNaEG8fHWHJabe";
     //   // AuthResponse response = vault.auth().unwrap(wrappingToken);
     //   //
     //   // System.out.println("1. clientToken  = " + response.getAuthClientToken() );
     //
     //   System.out.println("2. Write data to vault ");
     //   Map<String, Object> secrets = new HashMap<String, Object>();
     //
     //   secrets.put("key1", "world 2");
     //   secrets.put("other_value", "You can store multiple name/value pairs under a single key");
     //
     //   LogicalResponse writeResponse = vault.logical()
     //                                       .write(secrets_path, secrets);
     //
     //   System.out.println("3. Read data from vault ");
     //   Map<String, String>  data = vault.logical()
     //                     .read(secrets_path)
     //                     .getData();
     //
     //   for (Map.Entry<String,String> entry : data.entrySet())  {
     //        System.out.println("Binhnt: Key = " + entry.getKey() +  ", Value = " + entry.getValue());
     //   }
     //
     //
     // } catch (VaultException e) {
     //   System.out.println("Cannot create config ." + e.toString());
     //   return;
     // }
  }
}
