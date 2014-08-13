package com.graphconcern.cenozoic.support.util;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import org.bouncycastle.asn1.ASN1Sequence;
import org.bouncycastle.asn1.pkcs.RSAPrivateKeyStructure;
import org.bouncycastle.asn1.x509.RSAPublicKeyStructure;
import org.bouncycastle.crypto.AsymmetricBlockCipher;
import org.bouncycastle.crypto.AsymmetricCipherKeyPair;
import org.bouncycastle.crypto.BlockCipher;
import org.bouncycastle.crypto.CryptoException;
import org.bouncycastle.crypto.InvalidCipherTextException;
import org.bouncycastle.crypto.KeyGenerationParameters;
import org.bouncycastle.crypto.digests.MD5Digest;
import org.bouncycastle.crypto.digests.SHA256Digest;
import org.bouncycastle.crypto.generators.DESedeKeyGenerator;
import org.bouncycastle.crypto.generators.RSAKeyPairGenerator;
import org.bouncycastle.crypto.modes.CBCBlockCipher;
import org.bouncycastle.crypto.paddings.PaddedBufferedBlockCipher;
import org.bouncycastle.crypto.params.KeyParameter;
import org.bouncycastle.crypto.params.ParametersWithIV;
import org.bouncycastle.crypto.params.RSAKeyGenerationParameters;
import org.bouncycastle.crypto.params.RSAKeyParameters;
import org.bouncycastle.crypto.params.RSAPrivateCrtKeyParameters;
import org.bouncycastle.util.encoders.Base64;
import org.bouncycastle.util.encoders.Hex;
import org.bouncycastle.crypto.encodings.PKCS1Encoding;
import org.bouncycastle.crypto.engines.AESEngine;
import org.bouncycastle.crypto.engines.BlowfishEngine;
import org.bouncycastle.crypto.engines.DESedeEngine;
import org.bouncycastle.crypto.engines.RSAEngine;

public class CryptoAPI {
	
	/*** RSA utility ***/
	/*
	 * DO NOT CHANGE THIS DEFAULT VALUE otherwise it would break compatibility with other open sources and commercial security software.
	 */
	private static final BigInteger PUB_EXPONENT = BigInteger.valueOf(0x10001);	
	
	public BigInteger getDefaultPublicExponent() {
		return PUB_EXPONENT;
	}
	
	public AsymmetricCipherKeyPair generateRsaKeyPair() {
		SecureRandom r = new SecureRandom();
		RSAKeyPairGenerator pGen = new RSAKeyPairGenerator();
		RSAKeyGenerationParameters genParam = new RSAKeyGenerationParameters(PUB_EXPONENT, r, 1024, 25);
		pGen.init(genParam);
		return pGen.generateKeyPair();
	}
	
	public String getPrivatePEM(AsymmetricCipherKeyPair keyPair) {
		RSAPrivateCrtKeyParameters privateKey = (RSAPrivateCrtKeyParameters) keyPair.getPrivate();
		RSAPrivateKeyStructure pri = new RSAPrivateKeyStructure(privateKey.getModulus(), privateKey.getPublicExponent(), privateKey.getExponent(), 
				privateKey.getP(), privateKey.getQ(), privateKey.getDP(), privateKey.getDQ(), privateKey.getQInv());
		return pemEncode(new String(Base64.encode(pri.getDEREncoded())), true);
	}
	
	public String getPublicPEM(AsymmetricCipherKeyPair keyPair) {
		RSAKeyParameters publicKey = (RSAKeyParameters) keyPair.getPublic();
		RSAPublicKeyStructure pub = new RSAPublicKeyStructure(publicKey.getModulus(), publicKey.getExponent());
		return pemEncode(new String(Base64.encode(pub.getDEREncoded())), false);
	}
	
	public RSAPrivateKeyStructure getPrivateKey(String pemBlock) throws IOException {
		return new RSAPrivateKeyStructure(pemDecode(pemBlock));
	}
	
	public RSAPublicKeyStructure getPublicKey(String pemBlock) throws IOException  {
		return new RSAPublicKeyStructure(pemDecode(pemBlock));
	}
	
	private String pemEncode(String b64, boolean isPrivate) {
		StringBuffer sb = new StringBuffer();
		sb.append(isPrivate? "-----BEGIN RSA PRIVATE KEY-----" : "-----BEGIN RSA PUBLIC KEY-----");
		sb.append("\r\n");
		int i = 0;
		int len = b64.length();
		while (i < len) {
			int end = i + 60;
			if (end > len) end = len;
			sb.append(b64.substring(i, end));
			sb.append("\r\n");
			i = end;
		}
		sb.append(isPrivate? "-----END RSA PRIVATE KEY-----" : "-----END RSA PUBLIC KEY-----");
		sb.append("\r\n");
		return sb.toString();
	}
	
	private ASN1Sequence pemDecode(String b64) throws IOException {
		StringBuffer sb = new StringBuffer();
		BufferedReader reader = new BufferedReader(new StringReader(b64));
		while (true) {
			String line = reader.readLine();
			if (line == null) break;
			if (line.length() == 0) continue;
			if (line.contains("-----")) continue;
			sb.append(line);
		}
		byte[] b = Base64.decode(sb.toString());
		return (ASN1Sequence) ASN1Sequence.fromByteArray(b);
	}

	public byte[] rsaEncrypt(byte[] cleartext, BigInteger modulus) {		
		RSAKeyParameters pk = new RSAKeyParameters(false, modulus, PUB_EXPONENT);  
        AsymmetricBlockCipher engine = new PKCS1Encoding(new RSAEngine());
		engine.init(true, pk);
		
		try {
			return engine.processBlock(cleartext, 0, cleartext.length);
		} catch (InvalidCipherTextException e) {
			return null;
		}
	}
	
	public byte[] rsaEncrypt(byte[] cleartext, byte[] modulus) {		
		RSAKeyParameters pk = new RSAKeyParameters(false, new BigInteger(modulus), PUB_EXPONENT);  
        AsymmetricBlockCipher engine = new PKCS1Encoding(new RSAEngine());
		engine.init(true, pk);
		
		try {
			return engine.processBlock(cleartext, 0, cleartext.length);
		} catch (InvalidCipherTextException e) {
			return null;
		}
	}
	
	public byte[] rsaDecrypt(byte[] ciphertext, BigInteger modulus, BigInteger exponent) {		
		RSAKeyParameters sk = new RSAKeyParameters(true, modulus, exponent);
        AsymmetricBlockCipher engine = new PKCS1Encoding(new RSAEngine());
		engine.init(false, sk);		

		try {
			return engine.processBlock(ciphertext, 0, ciphertext.length);
		} catch (InvalidCipherTextException e) {
			return null;
		}
	}

	public byte[] rsaDecrypt(byte[] ciphertext, byte[] modulus, byte[] exponent) {		
		RSAKeyParameters sk = new RSAKeyParameters(true, new BigInteger(modulus), new BigInteger(exponent));
        AsymmetricBlockCipher engine = new PKCS1Encoding(new RSAEngine());
		engine.init(false, sk);		

		try {
			return engine.processBlock(ciphertext, 0, ciphertext.length);
		} catch (InvalidCipherTextException e) {
			return null;
		}
	}

	/*** Symmetric Encryption utility ***/
	
	/*
	 * Generate symmetric key (length: 128, 192, 256, 384, 512 bits)
	 */
	public byte[] generateSymmetricKey(int n) {
		switch (n) {
			case 192:
				return get3DesKey(192);
			case 256:
				return genLongerKey(2);		
			case 384:
				return genLongerKey(3);
			case 512:
				return genLongerKey(4);
			default:
				return get3DesKey(128);
		}
	}
	/*
	 * Create longer key by combining multiple 3DES keys
	 */
	private byte[] genLongerKey(int n) {
		ByteArrayOutputStream out = new ByteArrayOutputStream(32);
		try {
			for (int i=0; i < n; i++) {
				out.write(get3DesKey(128));
			}
		} catch (IOException e) {}
		return out.toByteArray();
	}	
	/*
	 * Generate 128 or 192 bit 3DES key
	 * Using the DES generator avoid weak keys
	 */
	private byte[] get3DesKey(int len) {
		SecureRandom r = new SecureRandom(long2bytes(System.currentTimeMillis()));
		KeyGenerationParameters kgp = new KeyGenerationParameters(r, len > 128? 192 : 128);
		DESedeKeyGenerator kg = new DESedeKeyGenerator();
		kg.init(kgp);
		return kg.generateKey();
	}
	
	public byte[] desEncrypt(byte[] cleartext, byte[] key) {
		return encrypt(cleartext, key, new DESedeEngine(), null);
	}
	public byte[] desEncrypt(byte[] cleartext, byte[] key, byte[] iv) {
		return encrypt(cleartext, key, new DESedeEngine(), iv);
	}
	
	public byte[] blowfishEncrypt(byte[] cleartext, byte[] key) {
		return encrypt(cleartext, key, new BlowfishEngine(), null);
	}
	public byte[] blowfishEncrypt(byte[] cleartext, byte[] key, byte[] iv) {
		return encrypt(cleartext, key, new BlowfishEngine(), iv);
	}
	
	public byte[] aesEncrypt(byte[] cleartext, byte[] key) {
		return encrypt(cleartext, key, new AESEngine(), null);
	}
	public byte[] aesEncrypt(byte[] cleartext, byte[] key, byte[] iv) {
		return encrypt(cleartext, key, new AESEngine(), iv);
	}
	
	public byte[] desDecrypt(byte[] ciphertext, byte[] key) {
		return decrypt(ciphertext, key, new DESedeEngine(), null);
	}	
	public byte[] desDecrypt(byte[] ciphertext, byte[] key, byte[] iv) {
		return decrypt(ciphertext, key, new DESedeEngine(), iv);
	}
	
	public byte[] blowfishDecrypt(byte[] ciphertext, byte[] key) {
		return decrypt(ciphertext, key, new BlowfishEngine(), null);
	}
	public byte[] blowfishDecrypt(byte[] ciphertext, byte[] key, byte[] iv) {
		return decrypt(ciphertext, key, new BlowfishEngine(), iv);
	}
	
	public byte[] aesDecrypt(byte[] ciphertext, byte[] key) {
		return decrypt(ciphertext, key, new AESEngine(), null);
	}
	public byte[] aesDecrypt(byte[] ciphertext, byte[] key, byte[] iv) {
		return decrypt(ciphertext, key, new AESEngine(), iv);
	}
	
	public byte[] encrypt(byte[] cleartext, byte[] key, BlockCipher engine, byte[] iv) {
		if (cleartext == null) return null;
		/* 
		 * Setup cipher engine, create a PaddedBufferedBlockCipher in CBC mode.
		 * Default block cipher uses PKCS7Padding.
		 */
		PaddedBufferedBlockCipher cipher = new PaddedBufferedBlockCipher(new CBCBlockCipher(engine));
		if (iv == null) {
			cipher.init(true, new KeyParameter(key));
		} else {
			cipher.init(true, new ParametersWithIV(new KeyParameter(key), iv));
		}		
		/*
		 * Minimum block size = 128 bytes
		 */
		int cipherBlockSize = cipher.getBlockSize();
		int inBlockSize = cipherBlockSize * 8;		
		if (inBlockSize < 128) inBlockSize = 128;
        int outBlockSize = cipher.getOutputSize(inBlockSize);
        byte[] inblock = new byte[inBlockSize];
        byte[] outblock = new byte[outBlockSize];        

        ByteArrayInputStream in = new ByteArrayInputStream(cleartext);
		ByteArrayOutputStream out = new ByteArrayOutputStream(cleartext.length);
        try {
            int inLen;
            int outLen;
            /*
             * If IV is not given, some random bytes will be inserted as a pseudo-IV.
             * Default size = cipherBlockSize/2 
             * i.e. 8 bytes
             */        
            if (iv == null) {
            	byte[] someBytes = getRandomBytes(cipherBlockSize/2);
                outLen = cipher.processBytes(someBytes, 0, someBytes.length, outblock, 0);
                if (outLen > 0) out.write(outblock, 0, outLen);
            } 
            while ((inLen=in.read(inblock, 0, inBlockSize)) > 0) {
                outLen = cipher.processBytes(inblock, 0, inLen, outblock, 0);
                if (outLen > 0) out.write(outblock, 0, outLen);
            }
			outLen = cipher.doFinal(outblock, 0);
			if (outLen > 0) out.write(outblock, 0, outLen);
			
		} catch (CryptoException e) { 
			return null; 
		}		
		return out.toByteArray();
	}
	
	public byte[] decrypt(byte[] ciphertext, byte[] key, BlockCipher engine, byte[] iv) {
		if (ciphertext == null) return null;

		PaddedBufferedBlockCipher cipher = new PaddedBufferedBlockCipher(new CBCBlockCipher(engine));
		if (iv == null) {
			cipher.init(false, new KeyParameter(key));
		} else {
			cipher.init(false, new ParametersWithIV(new KeyParameter(key), iv));
		}	

		int cipherBlockSize = cipher.getBlockSize();
		int inBlockSize = cipherBlockSize * 8;
		if (inBlockSize < 128) inBlockSize = 128;
		int outBlockSize = cipher.getOutputSize(inBlockSize);
        byte[] inblock = new byte[inBlockSize];
        byte[] outblock = new byte[outBlockSize];
		ByteArrayInputStream in = new ByteArrayInputStream(ciphertext);
		ByteArrayOutputStream out = new ByteArrayOutputStream(ciphertext.length);
		
		try {
			int inLen;
			int outLen;
			int skip = (iv == null) ? cipherBlockSize/2 : 0;

			while ((inLen=in.read(inblock, 0, inBlockSize)) > 0) {
                outLen = cipher.processBytes(inblock, 0, inLen, outblock, 0);
                if (outLen > 0) {
                	if (skip > 0) {
                		skip = skipPseudoIV(out, outblock, outLen, skip);
                	} else {
                		out.write(outblock, 0, outLen);
                	}
                }
            }
			outLen = cipher.doFinal(outblock, 0);
			if (outLen > 0) {
            	if (skip > 0) {
            		skip = skipPseudoIV(out, outblock, outLen, skip);
            	} else {
            		out.write(outblock, 0, outLen);
            	}
			}

		} catch (CryptoException e) { 
			return null; 
		}
		return out.toByteArray();
	}
	
	private int skipPseudoIV(ByteArrayOutputStream out, byte[] outblock, int len, int skip) {
		/*
		 * If IV is null, the first block contains a pseudo-IV.
		 * To restore the original clear text, it should be skipped.
		 */
		if (len > skip) {
			out.write(outblock, skip, len - skip);
			return 0;
		} else {
			return skip - len;
		}
	}
	
	public byte[] getRandomBytes(int n) {
	      SecureRandom random = new SecureRandom(long2bytes(System.currentTimeMillis()));
	      byte bytes[] = new byte[n];
	      random.nextBytes(bytes);
	      return bytes;
	}
	
	private byte[] long2bytes(long val) {
		byte[] results = new byte[8];

		for (int idx =7; idx >=0; --idx) {
			results[idx] = (byte) (val & 0xFF);
			val = val >> 8;
		}
		return results;
	}
	
	/*** HMAC SHA1 ***/
	
	public static String hmacSha1(String data, String key)  {
		try {
			SecretKeySpec secretKey = new SecretKeySpec(key.getBytes("UTF-8"), "HmacSHA1");
			Mac mac = Mac.getInstance("HmacSHA1");
	        mac.init(secretKey);
	        byte[] digest = mac.doFinal(data.getBytes("UTF-8"));
	        return new String(Base64.encode(digest));
		} catch (UnsupportedEncodingException e) {
			// just ignore as this is impossible
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		} catch (InvalidKeyException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	/*** Simple MD5 and SHA256 hashing ***/
	
	public static String getMD5(String data) {
		try {
			return getMD5(data.getBytes("UTF-8"));
		} catch (UnsupportedEncodingException e) {
			// just ignore as this is impossible
		}
		return null;
	}
	
	public static String getMD5(byte[] input) {
		MD5Digest md5 = new MD5Digest();
		md5.update(input, 0, input.length);
		byte[] digest = new byte[md5.getDigestSize()];
		md5.doFinal(digest, 0);
		return new String(Base64.encode(digest));
	}
	
	public static String getSHA256(String data) {
		try {
			byte[] input = data.getBytes("UTF-8");
			SHA256Digest hash = new SHA256Digest();
			hash.update(input, 0, input.length);
			byte[] digest = new byte[hash.getDigestSize()];
			hash.doFinal(digest, 0);
			return new String(Hex.encode(digest));
		} catch (UnsupportedEncodingException e) {
			// just ignore as this is impossible
		}
		return null;
	}

}
