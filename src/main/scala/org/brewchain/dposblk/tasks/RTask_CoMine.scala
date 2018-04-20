
package org.brewchain.dposblk.tasks

import org.fc.brewchain.p22p.node.Network
import org.fc.brewchain.p22p.utils.LogHelper

import onight.tfw.outils.serialize.UUIDGenerator
import onight.tfw.async.CallBack
import onight.tfw.otransio.api.beans.FramePacket
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import org.fc.brewchain.bcapi.crypto.BitMap
import org.brewchain.dposblk.pbgens.Dposblock.PSCoMineOrBuilder
import org.brewchain.dposblk.pbgens.Dposblock.PSCoMine
import org.brewchain.dposblk.pbgens.Dposblock.PDNodeOrBuilder
import org.brewchain.dposblk.pbgens.Dposblock.PRetCoMine
import org.brewchain.dposblk.utils.DConfig
import org.brewchain.dposblk.pbgens.Dposblock.DNodeState

//获取其他节点的term和logidx，commitidx
object RTask_CoMine extends LogHelper with BitMap {
  def runOnce(implicit network: Network): PDNodeOrBuilder = {
    Thread.currentThread().setName("RTask_Join");
    val join = PSCoMine.newBuilder().setDn(DCtrl.curDN()).build();
    val msgid = UUIDGenerator.generate();
    val cn = DCtrl.instance.cur_dnode;
    var fastNode: PDNodeOrBuilder = cn;
    var minCost: Long = Long.MaxValue;
    var maxBlockHeight: Long = 0;
    MDCSetBCUID(network)

    network.directNodes.filter { n => !DCtrl.coMinerByUID.contains(n.bcuid) }.map { n =>
      val start = System.currentTimeMillis();
      network.sendMessage("JINDOB", join, n, new CallBack[FramePacket] {
        def onSuccess(fp: FramePacket) = {
          log.debug("send JINDOB success:to " + n.uri + ",body=" + fp.getBody)
          val end = System.currentTimeMillis();
          val retjoin = PRetCoMine.newBuilder().mergeFrom(fp.getBody);
          if (retjoin.getRetCode() == 0) { //same message
            if (fastNode == null) {
              fastNode = retjoin.getDn;
            } else if (retjoin.getDn.getCurBlock >= fastNode.getCurBlock) {
              if (end - start < minCost) { //set the fast node
                minCost = end - start
                fastNode = retjoin.getDn;
              }
            }
            log.debug("get other nodeInfo:B=" + retjoin.getDn.getCurBlock);
            DCtrl.coMinerByUID.put(retjoin.getDn.getBcuid, retjoin.getDn);
          }
        }
        def onFailed(e: java.lang.Exception, fp: FramePacket) {
          log.debug("send JINDOB ERROR " + n.uri + ",e=" + e.getMessage, e)
        }
      })
    }
    log.debug("get nodes:count=" + DCtrl.coMinerByUID.size+",dposnetNodecount="+network.directNodeByBcuid.size);
    //remove off line
    DCtrl.coMinerByUID.filter(p => {
      network.nodeByBcuid(p._1) == network.noneNode
    }).map { p =>
      log.debug("remove Node:" + p._1);
      DCtrl.coMinerByUID.remove(p._1);
    }

    if (fastNode != cn && DCtrl.coMinerByUID.size >= network.directNodes.size * 2 / 3
      && cn.getCurBlock < fastNode.getCurBlock) {
      cn.setState(DNodeState.DN_SYNC_BLOCK)
      BlockSync.trySyncBlock(fastNode.getCurBlock, fastNode.getBcuid);
      fastNode
    } else if (cn.getCurBlock >= fastNode.getCurBlock) {
      log.debug("ready to become cominer is max:cur=" + cn.getCurBlock + ", net=" + fastNode.getCurBlock);
      cn.setState(DNodeState.DN_CO_MINER)
      cn
    } else {
      null
    }

  }

}
