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
import org.brewchain.dposblk.pbgens.Dposblock.PSCoMine
import org.brewchain.dposblk.pbgens.Dposblock.PRetCoMine
import org.brewchain.dposblk.pbgens.Dposblock.PDNodeOrBuilder
import org.brewchain.dposblk.pbgens.Dposblock.DNodeState
import org.brewchain.bcapi.exec.SRunner
import org.fc.brewchain.p22p.action.PMNodeHelper

//获取其他节点的term和logidx，commitidx
case class DTask_HeatBeat() extends SRunner with PMNodeHelper with LogHelper {
  def getName(): String = "HeatBeat-Dpos"

  def runOnce() = {
    //
    try {
      val network = DCtrl.dposNet();
      MDCSetBCUID(network)
      val messageid = UUIDGenerator.generate();
      MDCSetMessageID(messageid)
      checkDNodeLostInMiner(network);
      checkCoMinerLostInDnode(network);
    } catch {
      case e: Throwable =>
        log.error("SyncError:" + e.getMessage, e)
    } finally {
    }
  }
  def checkDNodeLostInMiner(network: Network): Unit = {
    try {
      val start = System.currentTimeMillis();
      val lostM = network.directNodeByBcuid.filter({ p =>
        !DCtrl.coMinerByUID.contains(p._1)
      })
      log.debug("HeatBeat:DNodeLostInMiner=" + lostM.size + ":" + lostM.foldLeft("")((a, B) => a + B._1 + ",") + ",MN=" + DCtrl.coMinerByUID.size + ",DN=" +
        network.directNodeByBcuid.size + ",PN=" + network.pendingNodeByBcuid.size);
      val cdl = new CountDownLatch(lostM.size)
      val join = PSCoMine.newBuilder().setDn(DCtrl.curDN())
        .build();
      var fastNode: PDNodeOrBuilder = DCtrl.curDN();
      var minCost: Long = Long.MaxValue;
      var maxBlockHeight: Long = 0;

      val msgid = UUIDGenerator.generate();
      lostM.map { n =>
        val start = System.currentTimeMillis();
        network.asendMessage("JINDOB", join, n._2, new CallBack[FramePacket] {
          def onSuccess(fp: FramePacket) = {
            try {
              val end = System.currentTimeMillis();
              val retjoin = if (fp.getBody != null) {
                PRetCoMine.newBuilder().mergeFrom(fp.getBody);
              } else if (fp.getFbody != null && fp.getFbody.isInstanceOf[PRetCoMine]) {
                fp.getFbody.asInstanceOf[PRetCoMine]
              } else {
                null;
              }
              MDCSetBCUID(network)
              if (retjoin != null && retjoin.getRetCode() == 0) { //same message
                log.debug("send JINDOB success:to " + n._2.uri + ",code=" + retjoin.getRetCode)
                if (fastNode == null) {
                  fastNode = retjoin.getDn;
                } else if (retjoin.getDn.getCurBlock > fastNode.getCurBlock) {
                  fastNode = retjoin.getDn;
                  minCost = end - start
                } else if (retjoin.getDn.getCurBlock == fastNode.getCurBlock) {
                  if (end - start < minCost) { //set the fast node
                    minCost = end - start
                    fastNode = retjoin.getDn;
                  }
                }
                log.debug("get HB-other nodeInfo:B=" + retjoin.getDn.getCurBlock + ",state=" + retjoin.getCoResult);
                if (retjoin.getCoResult == DNodeState.DN_CO_MINER) {
                  DCtrl.coMinerByUID.put(retjoin.getDn.getBcuid, retjoin.getDn);
                }
              } else {
                log.debug("send HB-JINDOB Failed " + n._2.uri + ",retobj=" + retjoin)
              }
            } finally {
              cdl.countDown()
            }
          }
          def onFailed(e: java.lang.Exception, fp: FramePacket) {
            cdl.countDown()
            log.debug("send HB-JINDOB ERROR " + n._2.uri + ",e=" + e.getMessage, e)
            network.removeDNode(n._2);
          }
        })
      }
      cdl.await();
    } catch {
      case t: Throwable =>
        log.error("error in HeatBeat:", t);
    }
  }

  def checkCoMinerLostInDnode(network: Network): Unit = {
    try {
      val start = System.currentTimeMillis();
      val lostM = DCtrl.coMinerByUID.filter({ p =>
        !network.directNodeByBcuid.contains(p._1)
      })
      log.debug("HeatBeat:CoMinerLostInDNode=" + lostM.size + ":" + lostM.foldLeft("")((a, B) => a + B._1 + ",") + ",MN=" + DCtrl.coMinerByUID.size + ",DN=" +
        network.directNodeByBcuid.size + ",PN=" + network.pendingNodeByBcuid.size);
      lostM.map(p => {
        DCtrl.coMinerByUID.remove(p._1)
        log.debug("drop comainer for not in dnode:" + p._1 + ",coadr=" + p._2.getCoAddress);
      })

    } catch {
      case t: Throwable =>
        log.error("error in HeatBeat:", t);
    }
  }
}
