
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
import org.brewchain.dposblk.Daos
import org.brewchain.dposblk.pbgens.Dposblock.PBlockEntry
import org.brewchain.account.util.ByteUtil
import org.apache.commons.codec.binary.Base64

//获取其他节点的term和logidx，commitidx
object DTask_MineBlock extends LogHelper with BitMap {
  def runOnce(implicit network: Network): Boolean = {
    this.synchronized {
      //    Thread.currentThread().setName("RTask_MineBlock");
      val msgid = UUIDGenerator.generate();
      val cn = DCtrl.instance.cur_dnode;
      val curtime = System.currentTimeMillis();
      if (DCtrl.checkMiner(cn.getCurBlock + 1, cn.getCoAddress, curtime,
        DConfig.BLK_EPOCH_SEC * 1000)) {
        MDCSetBCUID(network)
        val newblk = Daos.blkHelper.CreateNewBlock(DCtrl.termMiner().getMaxTnxEachBlock,
          ByteUtil.EMPTY_BYTE_ARRAY, ByteUtil.EMPTY_BYTE_ARRAY);
        val newblockheight = cn.getCurBlock + 1
        if (newblockheight != newblk.getHeader.getNumber) {
          log.debug("mining error: ch=" + newblockheight + ",dbh=" + newblk.getHeader.getNumber);
          false;
        } else {
          //        log.debug("MineNewBlock:" + newblk);
          log.debug("mining check ok :new block=" + newblockheight + ",CO=" + cn.getCoAddress
            + ",MaxTnx=" + DCtrl.termMiner().getMaxTnxEachBlock + ",hash=" + newblk.getHeader.getBlockHash);

          val newCoinbase = PSCoinbase.newBuilder()
            .setBlockHeight(newblockheight).setCoAddress(cn.getCoAddress)
            .setTermId(DCtrl.termMiner().getTermId)
            .setCoNodes(DCtrl.coMinerByUID.size)
            .setTermSign(DCtrl.termMiner().getSign)
            .setCoAddress(cn.getCoAddress)
            .setMineTime(curtime)
            .setMessageId(msgid)
            .setBcuid(cn.getBcuid)
            .setBlockEntry(PBlockEntry.newBuilder().setBlockHeight(newblockheight)
              .setCoinbaseBcuid(cn.getCoAddress).setSliceId(DCtrl.termMiner().getSliceId)
              .setBlockHeader(newblk.build().toByteString())
              .setSign("TOLIUBODOSIGN"))
            .setSliceId(DCtrl.termMiner().getSliceId)

          //          log.debug("TRACE::BLKSH=["+Base64.encodeBase64String(newCoinbase.getBlockHeader.getBlockHeader.toByteArray())+"]");

          cn.setLastDutyTime(System.currentTimeMillis());
          cn.setCurBlock(newblockheight)
          DCtrl.instance.syncToDB()

          network.dwallMessage("MINDOB", Left(newCoinbase.build()), msgid)

          true
        }
      } else {
        log.debug("waiting for my mine block:" + (cn.getCurBlock + 1) + ",CO=" + cn.getCoAddress
          + ",sign=" + DCtrl.termMiner().getSign);
        false
      }
    }
  }

}
