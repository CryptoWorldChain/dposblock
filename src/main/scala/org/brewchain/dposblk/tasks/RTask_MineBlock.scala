
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
import org.brewchain.dposblk.pbgens.Dposblock.PSCoinbase

//获取其他节点的term和logidx，commitidx
object RTask_MineBlock extends LogHelper with BitMap {
  def runOnce(implicit network: Network): Unit = {
    this.synchronized {
      //    Thread.currentThread().setName("RTask_MineBlock");
      val msgid = UUIDGenerator.generate();
      val cn = DCtrl.instance.cur_dnode;
      if (DCtrl.checkMiner(cn.getCurBlock + 1, cn.getCoAddress)) {
        MDCSetBCUID(network)
        val newblockheight = cn.getCurBlock + 1
        cn.setCurBlock(newblockheight)
        val newblock = PSCoinbase.newBuilder()
          .setBlockHeight(newblockheight).setCoAddress(cn.getCoAddress)
          .setTermId(DCtrl.termMiner().getTermId)
          .setCoNodes(DCtrl.coMinerByUID.size)
          .setTermSign(DCtrl.termMiner().getSign)
          .setMessageId(msgid)
          .setSliceId(0)
        network.dwallMessage("MINDOB", Left(newblock.build()), msgid)
      }
    }
  }

}
