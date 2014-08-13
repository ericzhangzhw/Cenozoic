package com.graphconcern.cenozoic.support.util;

import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.UUID;

import org.bouncycastle.crypto.AsymmetricBlockCipher;
import org.bouncycastle.crypto.AsymmetricCipherKeyPair;
import org.bouncycastle.crypto.InvalidCipherTextException;
import org.bouncycastle.crypto.encodings.PKCS1Encoding;
import org.bouncycastle.crypto.engines.RSAEngine;
import org.bouncycastle.crypto.generators.RSAKeyPairGenerator;
import org.bouncycastle.crypto.params.RSAKeyGenerationParameters;
import org.bouncycastle.crypto.params.RSAKeyParameters;

public class SimpleRsaCrypto {
	
	private static final BigInteger PUB_EXPONENT = BigInteger.valueOf(0x10001);	
	
	private String serial;
	private BigInteger publicKey, privateKey;
	boolean isPrivate = false;
	
	public SimpleRsaCrypto() {
		this.genkey();
	}
	
	public SimpleRsaCrypto(String serial, byte[] publicKey) {
		this.serial = serial;
		this.publicKey = new BigInteger(publicKey);
		this.privateKey = null;
		isPrivate = false;
	}
	
	public SimpleRsaCrypto(String serial, byte[] publicKey, byte[] privateKey) {
		this.serial = serial;
		this.publicKey = new BigInteger(publicKey);
		this.privateKey = new BigInteger(privateKey);
		isPrivate = true;
	}
	
	public String getSerial() {
		return serial;
	}
	
	public byte[] getPublicKey() {
		return publicKey == null? null : publicKey.toByteArray();
	}
	
	public byte[] getPrivateKey() {
		return privateKey == null? null : privateKey.toByteArray();
	}
	
	private void genkey() {
		serial = UUID.randomUUID().toString();
		SecureRandom r = new SecureRandom(UUID.randomUUID().toString().getBytes());
		RSAKeyPairGenerator pGen = new RSAKeyPairGenerator();
		RSAKeyGenerationParameters genParam = new RSAKeyGenerationParameters(PUB_EXPONENT, r, 1024, 25);
		pGen.init(genParam);
		AsymmetricCipherKeyPair pair = pGen.generateKeyPair();
		publicKey = ((RSAKeyParameters) pair.getPrivate()).getModulus();
		privateKey = ((RSAKeyParameters) pair.getPrivate()).getExponent();	
		isPrivate = true;
	}
	
	public byte[] encrypt(byte[] cleartext) {
		if (cleartext != null && publicKey != null) {
			RSAKeyParameters pk = new RSAKeyParameters(false, publicKey, PUB_EXPONENT);        
	        AsymmetricBlockCipher engine = new PKCS1Encoding(new RSAEngine());
			engine.init(true, pk);
			try {
				return engine.processBlock(cleartext, 0, cleartext.length);
			} catch (InvalidCipherTextException e) {}
		}
		return null;
	}

	public byte[] decrypt(byte[] ciphertext) {
		if (ciphertext != null && publicKey != null && privateKey != null) {
			RSAKeyParameters sk = new RSAKeyParameters(true, publicKey, privateKey);
	        AsymmetricBlockCipher engine = new PKCS1Encoding(new RSAEngine());
			engine.init(false, sk);
			try {
				return engine.processBlock(ciphertext, 0, ciphertext.length);
			} catch (InvalidCipherTextException e) {}
		}
		return null;
	}

}
