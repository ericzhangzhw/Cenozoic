package com.graphconcern.search.sdk.experiment;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

public class TestThread extends Thread {
	
	private static AtomicInteger instance = new AtomicInteger();
	private int instanceId;
	private boolean timeout = false;
	private boolean done = false;
	
	public TestThread() {
		instanceId = instance.incrementAndGet();
	}
	
	public void wakeUp() {
		if (!done) {
			System.out.println("Thread "+instanceId+"--- wake up call ---");
			timeout = false;
			LockSupport.unpark(this);
		}
	}
	
	public void run() {
		
		System.out.println("Thread "+instanceId+" parked");
		
		timeout = true;
		LockSupport.parkUntil(System.currentTimeMillis() + 3000);
		
		if (timeout) {
			System.out.println("Thread "+instanceId+" timeout");
		}
		
		System.out.println("Thread "+instanceId+" resumed");
		done = true;
		
	}

}
