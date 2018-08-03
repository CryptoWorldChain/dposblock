package org.brewchain.dposblk.tasks

import org.fc.brewchain.p22p.node.Network
import org.fc.brewchain.p22p.utils.LogHelper
import onight.tfw.outils.serialize.UUIDGenerator
import onight.tfw.async.CallBack
import onight.tfw.otransio.api.beans.FramePacket

import scala.collection.JavaConversions._
import org.brewchain.bcapi.gens.Oentity.OValue
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.TimeUnit
import org.brewchain.dposblk.utils.DConfig
import org.fc.brewchain.p22p.node.Networks
import java.util.concurrent.atomic.AtomicBoolean

object BlockSync extends LogHelper {

  val runCounter = new AtomicLong(0);
  val maxReqHeight = new AtomicLong(0);
  val running = new AtomicBoolean(false);
  def tryBackgroundSyncLogs(block_max_wanted: Int, fastNodeID: String)(implicit network: Network): Unit = {
    var runable = new Runnable() {
      def run() {
        BlockSync.trySyncBlock(block_max_wanted, fastNodeID);
      }
    }
    Scheduler.runManager(runable);
    //Scheduler.runManager(runable,0,100, TimeUnit.MILLISECONDS)
  }
  def trySyncBlock(block_max_maybe_wanted: Int, fastNodeID: String)(implicit network: Network): Unit = {
    log.debug("syncblocklog --> block_max_maybe_wanted:" + block_max_maybe_wanted);
    if (network.nodeByBcuid(fastNodeID) == network.noneNode) {
      log.debug("cannot sync log bcuid not found in dposnet:nodeid=" + fastNodeID + ":");
    } else {
      val cn = DCtrl.instance.cur_dnode;
      //
      //      log.debug("try sync block: Max block= " + block_max_wanted + ",cur=" + cn.getCurBlock + ",running=" + running.get)
      try {
        maxReqHeight.synchronized({
          if (maxReqHeight.get > block_max_maybe_wanted && cn.getCurBlock >= block_max_maybe_wanted) {
            log.debug("not need to sync block: Max block=" + block_max_maybe_wanted + ",maxreqheight=" + maxReqHeight.get + ",cur=" + cn.getCurBlock + ",running=" + running.get)
            return ;
          } else {
            maxReqHeight.set(block_max_maybe_wanted)
          }
        })
        log.debug("syncblocklog --> try sync block: want max block= " + block_max_maybe_wanted + ",maxreqheight=" + maxReqHeight.get + ",cur=" + cn.getCurBlock + ",running=" + running.get)
        var lastLogTime = 0L;
        var skipreq = false;
        while (!running.compareAndSet(false, true) && !skipreq) {
          try {
            if (System.currentTimeMillis() - lastLogTime > 10 * 1000) {
              log.debug("waiting for runnerSyncBatch:curheight=" + cn.getCurBlock + ",runCounter=" + runCounter.get + ",wantblock= " + block_max_maybe_wanted + ",maxreqheight=" + maxReqHeight.get)
              lastLogTime = System.currentTimeMillis()
            }
            this.synchronized(this.wait(DConfig.SYNCBLK_WAITSEC_NEXTRUN))
            if (maxReqHeight.get > block_max_maybe_wanted) {
              log.debug("not need to sync block.: Max block=" + block_max_maybe_wanted + ",maxreqheight=" + maxReqHeight.get + ",cur=" + cn.getCurBlock + ",running=" + running.get)
              skipreq = true;
            }
          } catch {
            case t: InterruptedException =>
            case e: Throwable =>
          }
        }

        if (maxReqHeight.get > block_max_maybe_wanted) {
          log.debug("not need to sync block.: Max block=" + block_max_maybe_wanted + ",maxreqheight=" + maxReqHeight.get + ",cur=" + cn.getCurBlock + ",running=" + running.get)
          skipreq = true;
        }
        if (!skipreq) {
          //request log.
          val block_max_wanted = Math.min(cn.getCurBlock + DConfig.MAX_SYNC_BLOCKS,block_max_maybe_wanted);
          val pagecount =
            ((block_max_wanted - cn.getCurBlock) / DConfig.SYNCBLK_PAGE_SIZE).asInstanceOf[Int]
          +(if ((block_max_wanted - cn.getCurBlock) % DConfig.SYNCBLK_PAGE_SIZE == 0) 1 else 0)

          //        val cdlcount = Math.min(RConfig.SYNCLOG_MAX_RUNNER, pagecount)
          var cc = cn.getCurBlock + 1;
          while (cc <= block_max_wanted) {
            log.debug("syncblocklog --> DTask_SyncBlock cc:"+ cc + " endidx:" + Math.min(cc + DConfig.SYNCBLK_PAGE_SIZE - 1, block_max_wanted))
            val runner = DTask_SyncBlock(startIdx = cc, endIdx =
              Math.min(cc + DConfig.SYNCBLK_PAGE_SIZE - 1, block_max_wanted),
              network = network, fastNodeID, runCounter)
            cc += DConfig.SYNCBLK_PAGE_SIZE
            var runed = false;
            runCounter.synchronized({
              if (runCounter.get < DConfig.SYNCBLK_MAX_RUNNER) {
                runCounter.incrementAndGet();
                Scheduler.runOnce(runner);
                runed = true;
                log.debug("syncblocklog --> sync task runned")
              }
            })
            var lastLogTime = 0L;
            log.debug("syncblocklog --> runCounter.get:" + runCounter.get + " DConfig.SYNCBLK_MAX_RUNNER:" + DConfig.SYNCBLK_MAX_RUNNER)
            while (runCounter.get >= DConfig.SYNCBLK_MAX_RUNNER) {
              //wait... for next runner
              try {
                if (System.currentTimeMillis() - lastLogTime > 10 * 1000) {
                  log.debug("syncblocklog --> waiting for runner:cur=" + runCounter.get + " MAX_RUNNER=" + DConfig.SYNCBLK_MAX_RUNNER)
                  lastLogTime = System.currentTimeMillis()
                }
                this.synchronized(this.wait(DConfig.SYNCBLK_WAITSEC_NEXTRUN))
              } catch {
                case t: InterruptedException =>
                case e: Throwable =>
              }
            }
            if (!runed) {
              log.debug("syncblocklog --> rerun sync task")
              runCounter.incrementAndGet();
              Scheduler.runOnce(runner);
            }
          }
          while (runCounter.get > 0) {
            if (System.currentTimeMillis() - lastLogTime > 10 * 1000) {
              log.debug("waiting for log syncs:" + runCounter.get);
              lastLogTime = System.currentTimeMillis()
            }
            this.synchronized(Thread.sleep(DConfig.SYNCBLK_WAITSEC_NEXTRUN))
          }
          log.debug("syncblocklog --> finished init follow up logs:" + DCtrl.curDN().getCurBlock);
        } else {
          log.debug("skip request follow up logs:" + DCtrl.curDN().getCurBlock + ",block_wanted=" + block_max_maybe_wanted
            + ",from=" + fastNodeID);
        }
      } finally {
        running.set(false)
      }
    }
    //
  }
}