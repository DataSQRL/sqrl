/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl;

import java.util.concurrent.CyclicBarrier;

import org.crac.CheckpointException;
import org.crac.Core;
import org.crac.RestoreException;
import org.crac.management.CRaCMXBean;

import com.datasqrl.cmd.RootCommand;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DatasqrlCMD {


	private static class CracDelegate {

		public CracResourceAdapter registerResource() {
			log.debug("Registering JVM checkpoint/restore callback for Spring-managed lifecycle beans");
			CracResourceAdapter resourceAdapter = new CracResourceAdapter();
			org.crac.Core.getGlobalContext().register(resourceAdapter);
			return resourceAdapter;
		}

		public void checkpointRestore() {
			log.info("Triggering JVM checkpoint/restore");
			try {
				Core.checkpointRestore();
			}
			catch (UnsupportedOperationException ex) {
				ex.printStackTrace();
			}
			catch (CheckpointException ex) {
				ex.printStackTrace();
			}
			catch (RestoreException ex) {
				ex.printStackTrace();
		}
		}
	}

	/**
	 * Resource adapter for Project CRaC, triggering a stop-and-restart cycle
	 * for Spring-managed lifecycle beans around a JVM checkpoint/restore.
	 * @since 6.1
	 * @see #stopForRestart()
	 * @see #restartAfterStop()
	 */
	private static class CracResourceAdapter implements org.crac.Resource {

		private CyclicBarrier barrier;

		@Override
		public void beforeCheckpoint(org.crac.Context<? extends org.crac.Resource> context) {
			// A non-daemon thread for preventing an accidental JVM shutdown before the checkpoint
			initBarrier();

			Thread thread = new Thread(() -> {
				awaitPreventShutdownBarrier();
				// Checkpoint happens here
				awaitPreventShutdownBarrier();
			}, "prevent-shutdown");

			thread.setDaemon(false);
			thread.start();
		}

		private void initBarrier() {
			this.barrier = new CyclicBarrier(2);
		}

		@Override
		public void afterRestore(org.crac.Context<? extends org.crac.Resource> context) {
			log.info("Restarting Spring-managed lifecycle beans after JVM restore");

			if(barrier != null) {
				barrier.reset();
			}
			// Barrier for prevent-shutdown thread not needed anymore
			this.barrier = null;

				log.info("Spring-managed lifecycle restart completed (restored JVM running for " +
						CRaCMXBean.getCRaCMXBean().getUptimeSinceRestore() + " ms)");
		}

		private void awaitPreventShutdownBarrier() {
			try {
				if (this.barrier != null) {
					this.barrier.await();
				}
			}
			catch (Exception ex) {
				log.trace("Exception from prevent-shutdown barrier", ex);
			}
		}
	}
	
  public static void main(String[] args) {
    RootCommand rootCommand = new RootCommand();
    rootCommand.getCmd().parseArgs(args);
    
if(    rootCommand.isWaitForCracEvent() ) {
    System.out.println("Enabling resource adapter for Project CRaC");

    CracDelegate delegate = new CracDelegate();
	CracResourceAdapter cracResource = delegate.registerResource();
	cracResource.initBarrier();
	
    int exitCode = rootCommand.getCmd().execute(args);
    exitCode += (rootCommand.getStatusHook().isSuccess()?0:1);
    
    System.out.println("Awaiting crac");
    cracResource.awaitPreventShutdownBarrier(); 
    

    System.exit(exitCode);
} else {
    int exitCode = rootCommand.getCmd().execute(args);
    exitCode += (rootCommand.getStatusHook().isSuccess()?0:1);
    System.exit(exitCode);
}
  }

}