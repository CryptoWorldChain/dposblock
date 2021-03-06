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
import java.util.concurrent.atomic.AtomicBoolean
import org.fc.brewchain.p22p.node.Node
import org.brewchain.dposblk.utils.DConfig

//获取其他节点的term和logidx，commitidx
case class DTask_HeatBeat() extends SRunner with PMNodeHelper with LogHelper {
  def getName(): String = "HeatBeat-Dpos"

  var onScheduled = false;
  val running = new AtomicBoolean(false);
  def runOnce() = {
    //
    if (running.compareAndSet(false, true)) {
      try {
        val network = DCtrl.dposNet();
        MDCSetBCUID(network)
        val messageid = UUIDGenerator.generate();
        MDCSetMessageID(messageid)
        checkDNodeLostInMiner(network);
        checkCoMinerLostInDnode(network);
        checkCoMiner(network);
      } catch {
        case e: Throwable =>
          log.error("SyncError:" + e.getMessage, e)
      } finally {
        running.set(false);
      }
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
      cdl.await(DConfig.HEATBEAT_TICK_SEC, TimeUnit.SECONDS);
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

  def checkCoMiner(network: Network): Unit = {
    try {
      val start = System.currentTimeMillis();
      log.debug("checkCoMiner=:" + DCtrl.coMinerByUID.foldLeft("")((a, B) => a + B._1 + ",") + ",MN=" + DCtrl.coMinerByUID.size + ",DN=" +
        network.directNodeByBcuid.size + ",PN=" + network.pendingNodeByBcuid.size);
      val join = PSCoMine.newBuilder().setDn(DCtrl.curDN())
        .build();
      DCtrl.coMinerByUID.map { mn =>
        val start = System.currentTimeMillis();
        val pn = network.directNodeByBcuid.get(mn._1);
        if (pn != None && pn != network.noneNode) {
          val n = pn.get
          val cdl = new CountDownLatch(1);
          network.asendMessage("JINDOB", join, n, new CallBack[FramePacket] {
            def onSuccess(fp: FramePacket) = {
              cdl.countDown();
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
                if (retjoin != null && retjoin.getRetCode() == 0
                  && retjoin.getDn.getCoAddress.equals(n.v_address)
                  && retjoin.getDn.getBcuid.equals(n.bcuid)) { //same message
                  log.debug("send JINDOB success:to " + n.uri + ",bcuid=" + n.bcuid + ",code=" + retjoin.getRetCode)
                  log.debug("get SYNC-JINDOB nodeInfo:B=" + retjoin.getDn.getCurBlock + ",state=" + retjoin.getCoResult);
                  if (retjoin.getDn.getTermId < DCtrl.termMiner().getTermId &&
                    retjoin.getDn.getCurBlock < DCtrl.curDN().getCurBlock - 10
                    && DCtrl.termMiner().getMinerQueueList.filter { x => x.getMinerCoaddr.equals(retjoin.getDn.getCoAddress) }.size <= 0) {
                    log.debug("remove minder node:B=" + retjoin.getDn.getCurBlock
                      + ",curblock=" + DCtrl.curDN().getCurBlock
                      + ",state=" + retjoin.getCoResult
                      + ",termid=" + retjoin.getDn.getTermId + ",curtermid=" + DCtrl.termMiner().getTermId);
                    DCtrl.coMinerByUID.remove(n.bcuid);
                  } else if (retjoin.getCoResult == DNodeState.DN_CO_MINER) {
                    DCtrl.coMinerByUID.put(retjoin.getDn.getBcuid, retjoin.getDn);
                  }
                } else {
                  log.debug("send SYNC-JINDOB Failed uri=" + n.uri + ",bcuid=" + n.bcuid+",code="+retjoin.getRetCode())
                }
              } finally {
              }
            }
            def onFailed(e: java.lang.Exception, fp: FramePacket) {
              cdl.countDown();
              log.debug("send SYNC-JINDOB ERROR " + n.uri + ",bcuid=" + n.bcuid + ",e=" + e.getMessage, e)
              DCtrl.coMinerByUID.remove(n.bcuid);
            }
          })
          try {
            cdl.await(10, TimeUnit.SECONDS);
          } catch {
            case t: Throwable =>
              log.debug("send SYNC-JINDOB ERROR " + n.uri + ",bcuid=" + n.bcuid + ",e=" + t)
              DCtrl.coMinerByUID.remove(n.bcuid);
          }
        } else { //not found in dnode.
          DCtrl.coMinerByUID.remove(mn._1);
        }
      }

    } catch {
      case t: Throwable =>
        log.error("error in HeatBeat:", t);
    }
  }
  def trySyncMinerInfo(lostM: Iterable[Node], network: Network): Unit = {
    val join = PSCoMine.newBuilder().setDn(DCtrl.curDN())
      .build();
    var fastNode: PDNodeOrBuilder = DCtrl.curDN();
    var minCost: Long = Long.MaxValue;
    var maxBlockHeight: Long = 0;

    log.debug("trySyncMinerInfo:lostMsize=" + lostM.size + ",lostM=" + lostM.foldLeft("")((a, b) => a + "," + b.bcuid));
    lostM.map { n =>
      val start = System.currentTimeMillis();
      network.asendMessage("JINDOB", join, n, new CallBack[FramePacket] {
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
              log.debug("send JINDOB success:to " + n.uri + ",bcuid=" + n.bcuid + ",code=" + retjoin.getRetCode)
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
              log.debug("get SYNC-JINDOB nodeInfo:B=" + retjoin.getDn.getCurBlock + ",state=" + retjoin.getCoResult);
              if (retjoin.getCoResult == DNodeState.DN_CO_MINER) {
                DCtrl.coMinerByUID.put(retjoin.getDn.getBcuid, retjoin.getDn);
              }
            } else {
              log.debug("send SYNC-JINDOB Failed " + n.uri + ",bcuid=" + n.bcuid + ",retobj=" + retjoin)
            }
          } finally {
          }
        }
        def onFailed(e: java.lang.Exception, fp: FramePacket) {
          log.debug("send SYNC-JINDOB ERROR " + n.uri + ",bcuid=" + n.bcuid + ",e=" + e.getMessage, e)
        }
      })
    }
  }
}
