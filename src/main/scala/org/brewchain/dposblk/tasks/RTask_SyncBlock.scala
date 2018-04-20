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
import org.brewchain.dposblk.pbgens.Dposblock.PSSyncBlocks
import org.brewchain.dposblk.pbgens.Dposblock.PRetSyncBlocks
import org.brewchain.dposblk.pbgens.Dposblock.PBlockEntry
import org.brewchain.dposblk.Daos

//获取其他节点的term和logidx，commitidx
case class RTask_SyncBlock(startIdx: Int, endIdx: Int,
    network: Network, fastNodeID: String,
    runCounter: AtomicLong) extends SRunner with LogHelper {
  def getName(): String = "SyncBlock:" + startIdx + "-" + (endIdx)

  def runOnce() = {
    //
    try {
      val messageid = UUIDGenerator.generate();
      val sync = PSSyncBlocks.newBuilder().setStartId(startIdx)
        .setEndId(endIdx).setDn(DCtrl.curDN()).setMessageId(messageid).build()
      val start = System.currentTimeMillis();
      val n = network.nodeByBcuid(fastNodeID);
      if (n == null) {
        log.warn("cannot found node from Network:" + network.netid + ",bcuid=" + fastNodeID)
      }
      
      network.sendMessage("SYNDOB", sync, n, new CallBack[FramePacket] {
        def onSuccess(fp: FramePacket) = {
          val end = System.currentTimeMillis();
          log.debug("send SYNDOB success:to " + n.uri + ",cost=" + (end - start))
          val ret = PRetSyncBlocks.newBuilder().mergeFrom(fp.getBody);
          if (ret.getRetCode() == 0) { //same message
            var maxid: Long = 0
            val realmap = ret.getBlockHeadersList.map { b => (b, PBlockEntry.newBuilder().mergeFrom(b)) }
              .filter { p => p._2.getBlockHeight >= startIdx && p._2.getBlockHeight <= endIdx }
            //            if (realmap.size() == endIdx - startIdx + 1) {
            realmap.map { p =>
              val b = p._1;
              val loge = p._2;
              log.debug("get bock height=" + loge.getBlockHeight);
              Daos.dposdb.put("D" + loge.getBlockHeight, OValue.newBuilder()
                  .setCount(b.getBlockHeight)
                  .setInfo(b.getSign)
                  .setNonce(b.getSliceId)
                  .setSecondKey(b.getCoinbaseBcuid)
                  .setExtdata(b.getBlockHeader).build())
              log.info("set sync bock height=" + loge.getBlockHeight + ".........[OK]");
              if (loge.getBlockHeight > maxid) {
                maxid = loge.getBlockHeight;
              }
            }
            
            //!! DCtrl.instance.updateLastApplidId(maxid);
            //            } else {
            //              log.warn("cannot get enough entries:wanted:" + startIdx + "-->" + endIdx + ",returnsize=" +
            //                ret.getEntriesList.size() + ",after Filter=" + realmap.size);
            //            }
          }
        }
        def onFailed(e: java.lang.Exception, fp: FramePacket) {
          log.debug("send SYNRAF ERROR " + n.uri + ",e=" + e.getMessage, e)
        }
      })
    } catch {
      case e: Throwable =>
        log.error("SyncError:" + e.getMessage, e)
    } finally {
      runCounter.decrementAndGet();
    }
  }
}
