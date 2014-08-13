package com.graphconcern.search.sdk.experiment;

import java.util.concurrent.locks.LockSupport;


public class ThreadPark  {
	
	public static void main(String[] args) {
		
		ThreadPark simpleTest = new ThreadPark();
		
//		simpleTest.wakeMeUp();
		
		simpleTest.parkingThreads();
	}
	
	/*
	 * This example parks the main thread and ask a background thread to wake it up
	 */
	public void wakeMeUp() {
		System.out.println("starting up a background thread");
		Thread current = Thread.currentThread();
		System.out.println("----->"+current.getName());
		
		WakeUp b = new WakeUp(current);
		b.start();

		LockSupport.park();
		
        System.out.println("Total is: " + b.total);
		
	}
	/*
	 * This example starts two threads that park and wait for wake-up call from the main thread
	 */
	public void parkingThreads() {
		System.out.println("test started");
		
		
		TestThread test1 = new TestThread();
		TestThread test2 = new TestThread();
		
		test1.start();
		test2.start();

		try {
			Thread.sleep(200);
		} catch (InterruptedException e) {}
		
		test1.wakeUp();
		
		try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {}
		
		test2.wakeUp();
	}
	
	private class WakeUp extends Thread {
		
	    int total = 0;
	    Thread x;
	    
	    public WakeUp(Thread x) {
	    	this.x = x;
	    }
	    
	    @Override
	    public void run(){
            for(int i=0; i<10 ; i++){
                total += 30;
                System.out.println(total+" - "+i);
                try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
            }
            LockSupport.unpark(x);
            /*
             * Unpark again to see exception
             */
            for(int i=0; i<10 ; i++){
                total += 30;
                System.out.println(total+" - "+i);
                try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
            }
            LockSupport.unpark(x);

	    }
        
		
	}
	

}
