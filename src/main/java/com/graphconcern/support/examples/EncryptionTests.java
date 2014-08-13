package com.graphconcern.support.examples;

import java.io.IOException;

import org.bouncycastle.asn1.pkcs.RSAPrivateKeyStructure;
import org.bouncycastle.asn1.x509.RSAPublicKeyStructure;
import org.bouncycastle.crypto.AsymmetricCipherKeyPair;
import org.bouncycastle.crypto.params.RSAKeyParameters;
import org.bouncycastle.crypto.params.RSAPrivateCrtKeyParameters;
import com.graphconcern.support.util.CryptoAPI;
import com.graphconcern.support.util.Utility;

public class EncryptionTests {
	
	public static void main(String[] args) throws IOException {
		Utility util = new Utility();
		CryptoAPI crypto = new CryptoAPI();
		/*
		 * Generate a RSA key-pair
		 */
		AsymmetricCipherKeyPair pair = crypto.generateRsaKeyPair();
		
		RSAPrivateCrtKeyParameters privateKey = (RSAPrivateCrtKeyParameters) pair.getPrivate();
		RSAKeyParameters publicKey = (RSAKeyParameters) pair.getPublic();
		
		System.out.println("--- Generated private key ---");
		byte[] skModulus = privateKey.getModulus().toByteArray();
		byte[] skExponent = privateKey.getExponent().toByteArray();
		
		System.out.println(util.bytes2hex(skModulus));
		System.out.println(util.bytes2hex(skExponent));
		
		System.out.println("--- Generated public key ---");
		byte[] pkModulus = publicKey.getModulus().toByteArray();
		byte[] pkExponent = publicKey.getExponent().toByteArray();
		
		System.out.println(util.bytes2hex(pkModulus));
		System.out.println(util.bytes2hex(pkExponent));
		
		System.out.println("");
		
		/*
		 * Save key-pair as human readable PEM files
		 */
		String priPem = crypto.getPrivatePEM(pair);
		System.out.println(priPem);
		
		System.out.println("");
		
		String pubPem = crypto.getPublicPEM(pair);
		System.out.println(pubPem);
		
		/*
		 * Reconstruct key-pair from PEM files
		 */
		System.out.println("--- Reading PEM files ---");
		
		RSAPrivateKeyStructure x = crypto.getPrivateKey(priPem);
		RSAPublicKeyStructure y = crypto.getPublicKey(pubPem);

		System.out.println("--- Restoring private key ---");
		System.out.println(util.bytes2hex(x.getModulus().toByteArray()));
		System.out.println(util.bytes2hex(x.getPrivateExponent().toByteArray()));
		System.out.println(util.bytes2hex(x.getPublicExponent().toByteArray()));
		
		System.out.println("--- Restoring public key ---");
		System.out.println(util.bytes2hex(y.getModulus().toByteArray()));
		System.out.println(util.bytes2hex(y.getPublicExponent().toByteArray()));
			
		System.out.println("\n--- Confirm library integrity with a simple encryption test ---");
		
		String helloWorld = "hello world 系統工程學院 !!!";
		
		System.out.println("cleartext (hex value) = "+util.bytes2hex(util.getUTF(helloWorld)));
		
		byte[] encrypted = crypto.rsaEncrypt(util.getUTF(helloWorld), y.getModulus());
		
		byte[] decrypted = crypto.rsaDecrypt(encrypted, x.getModulus(), x.getPrivateExponent());
		
		String result = util.getUTF(decrypted);
		/*
		 * The only "use case" of RSA encryption is for key transfer so the 128 payload limitation is not a problem
		 */
		System.out.println("Cleartext = ("+helloWorld+")\nCiphertext = "+util.bytes2hex(encrypted)+"\nCiphertext length = "+encrypted.length+" bytes\nDecrypted = ("+result+")");
		
		/*
		 * Let's do some symmetric encryption
		 */
		System.out.println("\n--- Symmetric encryption/decryption tests ---");
		int keyLen = 256;
		int iterations = 1000;

		byte[] key = crypto.generateSymmetricKey(keyLen);
		System.out.println(keyLen+"-bit key="+util.bytes2hex(key));
		
		long t1 = System.currentTimeMillis();
				
		for (int i=0; i < iterations; i++) {
			/*
			 * Encryption with pseudo-IV where the library will insert 8 bytes of random in the beginning of the byte buffer
			 * to ensure that repeated encryption of the same bytes produces different ciphertext.
			 */
			byte[] ciphertext = crypto.aesEncrypt(util.getUTF(helloWorld), key);	// without initial vector (IV)
			/*
			 * Encryption with IV
			 * When using IV, the IV should be transported to the receiving side for decryption.
			 * Incorrect IV will produce corrupted result but it will not throw a crypto exception.
			 * This kind of bug is kind to trace so please set the correct IV for both sender and recipient.
			 */
//			byte[] ciphertext = crypto.aesEncrypt(util.getUTF(helloWorld), key, util.getUTF("0123456789123456"));
			if (i == iterations - 1) 
				System.out.println("ciphertext="+util.bytes2hex(ciphertext));
			
			byte[] cleartext = crypto.aesDecrypt(ciphertext, key);	// without initial vector (IV)
//			byte[] cleartext = crypto.aesDecrypt(ciphertext, key, util.getUTF("0123456789123456"));
			if (i == iterations - 1) {
				System.out.println("decrypted (hex value) = "+util.bytes2hex(cleartext));
				System.out.println("decrypted=("+util.getUTF(cleartext)+")");
			}
		}
		
		long diff = System.currentTimeMillis() - t1;
		
		System.out.println("Symmetric encrypt/decrypt ("+iterations+" times) took "+diff+" ms");
	}

}
