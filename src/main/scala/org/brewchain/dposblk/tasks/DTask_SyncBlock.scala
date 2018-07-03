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
import org.fc.brewchain.p22p.action.PMNodeHelper
import org.brewchain.bcapi.exec.SRunner

//获取其他节点的term和logidx，commitidx
case class DTask_SyncBlock(startIdx: Int, endIdx: Int,
    network: Network, fastNodeID: String, 
    runCounter: AtomicLong) extends SRunner with PMNodeHelper with LogHelper {
  def getName(): String = "SyncBlock:" + startIdx + "-" + (endIdx)

  def runOnce() = {
    //
    try {
      MDCSetBCUID(DCtrl.dposNet())
      val messageid = UUIDGenerator.generate();
      MDCSetMessageID(messageid)

      val sync = PSSyncBlocks.newBuilder().setStartId(startIdx)
        .setEndId(endIdx).setDn(DCtrl.curDN()).setMessageId(messageid).build()
      val start = System.currentTimeMillis();
      val n = network.nodeByBcuid(fastNodeID);
      if (n == null) {
        log.warn("cannot found node from Network:" + network.netid + ",bcuid=" + fastNodeID)
      } else {

        network.sendMessage("SYNDOB", sync, n, new CallBack[FramePacket] {
          def onSuccess(fp: FramePacket) = {
            val end = System.currentTimeMillis();
            MDCSetBCUID(DCtrl.dposNet());
            MDCSetMessageID(messageid)
            try {
              if (fp.getBody == null) {
                //sync Error.
                log.debug("send SYNDOB error:to " + fastNodeID + ",cost=" + (end - start) + ",s=" + startIdx + ",e=" + endIdx + ",ret=null")
              } else {
                val ret = PRetSyncBlocks.newBuilder().mergeFrom(fp.getBody);
                log.debug("send SYNDOB success:to " + fastNodeID + ",cost=" + (end - start) + ",s=" + startIdx + ",e=" + endIdx + ",ret=" +
                  ret.getRetCode + ",count=" + ret.getBlockHeadersCount)

                if (ret.getRetCode() == 0) { //same message

                  var maxid: Int = 0
                  val realmap = ret.getBlockHeadersList.filter { p => p.getBlockHeight >= startIdx && p.getBlockHeight <= endIdx }
                  //            if (realmap.size() == endIdx - startIdx + 1) {
                  log.debug("realBlockCount=" + realmap.size);
                  realmap.map { b =>
                    val (acceptedHeight,blockwanted) = DCtrl.saveBlock(b);
                    if (acceptedHeight == b.getBlockHeight) {
                      log.debug("sync block height ok=" + b.getBlockHeight + ",dbh=" + acceptedHeight);
                    } else {
                      log.debug("sync block height failed=" + b.getBlockHeight + ",dbh=" + acceptedHeight);
                    }
                    if (acceptedHeight > maxid) {
                      maxid = acceptedHeight;
                    }
                  }
                  DCtrl.instance.updateBlockHeight(maxid)
                }
              }
            } catch {
              case t: Throwable =>
                log.warn("error In SyncBlock:" + t.getMessage, t);
            }
          }
          def onFailed(e: java.lang.Exception, fp: FramePacket) {
            val end = System.currentTimeMillis();
            MDCSetBCUID(DCtrl.dposNet());
            MDCSetMessageID(messageid)
            log.debug("send SYNDOB ERROR :to " + fastNodeID + ",cost=" + (end - start) + ",s=" + startIdx + ",e=" + endIdx + n.uri + ",e=" + e.getMessage, e)
          }
        })
      }
    } catch {
      case e: Throwable =>
        log.error("SyncError:" + e.getMessage, e)
    } finally {
      runCounter.decrementAndGet();
    }
  }
}
