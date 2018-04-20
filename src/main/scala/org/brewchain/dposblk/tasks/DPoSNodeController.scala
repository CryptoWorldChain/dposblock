package org.brewchain.dposblk.tasks

import org.fc.brewchain.p22p.node.Network
import org.fc.brewchain.p22p.utils.LogHelper
import org.brewchain.bcapi.gens.Oentity.OValue
import org.apache.commons.lang3.StringUtils
import org.fc.brewchain.p22p.node.Node
import scala.collection.mutable.Map
import java.util.concurrent.atomic.AtomicLong
import org.fc.brewchain.bcapi.JodaTimeHelper
import org.brewchain.dposblk.pbgens.Dposblock.PDNode
import java.util.concurrent.atomic.AtomicInteger
import org.brewchain.dposblk.Daos
import org.brewchain.dposblk.pbgens.Dposblock.DNodeState
import org.brewchain.dposblk.pbgens.Dposblock.PSDutyTermVote
import org.brewchain.dposblk.pbgens.Dposblock.PDNodeOrBuilder
import org.brewchain.dposblk.pbgens.Dposblock.PSDutyTermVoteOrBuilder
import org.brewchain.dposblk.pbgens.Dposblock.PDutyTermResult

//投票决定当前的节点
case class DPosNodeController(network: Network) extends SRunner with LogHelper {
  def getName() = "DCTRL"
  val DPOS_NODE_DB_KEY = "CURRENT_DPOS_KEY";
  var cur_dnode: PDNode.Builder = PDNode.newBuilder()
  var term_Miner: PSDutyTermVote.Builder = PSDutyTermVote.newBuilder();
  var vote_Request: PSDutyTermVote.Builder = PSDutyTermVote.newBuilder();

  val cur_blk_height = new AtomicInteger(1)

  def nextBlock(): Long = {
    cur_blk_height.incrementAndGet()
  }

  def retsetBlockHeight(hi: Int): Unit = {
    cur_blk_height.set(hi)
  }

  def updateVoteReq(pbo: PSDutyTermVote): Unit = {
    vote_Request = pbo.toBuilder()
    cur_dnode.setNodeCount(vote_Request.getCoNodes)
    syncToDB();
  }
  def loadNodeFromDB(): PDNode.Builder = {
    val ov = Daos.dposdb.get(DPOS_NODE_DB_KEY).get
    val root_node = network.root();
    if (ov == null) {
      cur_dnode.setBcuid(root_node.bcuid)
        .setCurBlock(1).setCoAddress(root_node.v_address)
      Daos.dposdb.put(DPOS_NODE_DB_KEY,
        OValue.newBuilder().setExtdata(cur_dnode.build().toByteString()).build())
    } else {
      cur_dnode.mergeFrom(ov.getExtdata)
      if (!StringUtils.equals(cur_dnode.getBcuid, root_node.bcuid)) {
        log.warn("load from dnode info not equals with pzp node:" + cur_dnode + ",root=" + root_node)
      } else {
        log.info("load from db:OK" + cur_dnode)
      }
    }
    cur_dnode
  }
  def syncToDB() {
    Daos.dposdb.put(DPOS_NODE_DB_KEY,
      OValue.newBuilder().setExtdata(cur_dnode.build().toByteString()).build())
  }
  def updateBlockHeight(blockHeight: Int) = {
    if (cur_dnode.getCurBlock < blockHeight) {
      cur_dnode.setCurBlock(blockHeight)
      syncToDB()
    }
  }
  def runOnce() = {
    Thread.currentThread().setName("DCTRL");
    implicit val _net = network
    MDCSetBCUID(network);
    MDCRemoveMessageID()
    var continue = true;
    while (continue) {
      try {
        continue = false;
        log.info("DCTRL.RunOnce:S=" + cur_dnode.getState + ",B=" + cur_dnode.getCurBlock
          + ",CA=" + cur_dnode.getCoAddress
          + ",N=" + cur_dnode.getNodeCount
          + ",RN=" + network.bitenc.bits.bitCount
          + ",TN=" + cur_dnode.getTxcount + ",DU=" + cur_dnode.getDutyUid
          + ",NextSec=" + JodaTimeHelper.secondFromNow(cur_dnode.getDutyEndMs)
          + ",SecPass=" + JodaTimeHelper.secondFromNow(cur_dnode.getLastDutyTime));
        cur_dnode.getState match {
          case DNodeState.DN_INIT =>
            //tell other I will join
            loadNodeFromDB();
            continue = RTask_CoMine.runOnce match {
              case n: PDNode if n == cur_dnode =>
                log.debug("dpos cominer init ok:" + n);
                true;
              case n: PDNode if n != cur_dnode =>
                log.debug("dpos waiting for init:" + n);
                false
              case x @ _ =>
                log.debug("not ready");
                false
            }
          case DNodeState.DN_CO_MINER =>
            if (RTask_DutyTermVote.runOnce) {
              continue = true;
              cur_dnode.setState(DNodeState.DN_DUTY_MINER);
            }
          case DNodeState.DN_DUTY_MINER =>
            RTask_MineBlock.runOnce
            if (cur_dnode.getCurBlock > vote_Request.getBlockRange.getEndBlock) {
              //continue = true;
              //
              Thread.sleep(Math.abs((Math.random() * 1000).asInstanceOf[Long]) + 10);
              cur_dnode.setState(DNodeState.DN_CO_MINER);
            }
          case DNodeState.DN_SYNC_BLOCK =>
            RTask_CoMine.runOnce
          case DNodeState.DN_BAKCUP =>
            RTask_CoMine.runOnce
          case _ =>
            log.warn("unknow State:" + cur_dnode.getState);

        }

      } catch {
        case e: Throwable =>
          log.debug("raft sate managr :Error", e);
      } finally {
        MDCRemoveMessageID()
      }
    }
  }
}

object DCtrl {
  var instance: DPosNodeController = DPosNodeController(null);
  def dposNet(): Network = instance.network;
  //  val superMinerByUID: Map[String, PDNode] = Map.empty[String, PDNode];
  val coMinerByUID: Map[String, PDNode] = Map.empty[String, PDNode];
  def curDN(): PDNode.Builder = instance.cur_dnode
  def termMiner(): PSDutyTermVoteOrBuilder = instance.term_Miner
  def voteRequest(): PSDutyTermVote.Builder = instance.vote_Request

  //  def curTermMiner(): PSDutyTermVoteOrBuilder = instance.term_Miner

  def isReady(): Boolean = {
    instance.network != null &&
      instance.cur_dnode.getStateValue > DNodeState.DN_INIT_VALUE
  }

  def checkMiner(block: Int, coaddr: String): Boolean = {
    minerByBlockHeight(block) match {
      case Some(n) =>
        coaddr.equals(n)
      case None =>
        false
    }
  }
  def minerByBlockHeight(block: Int): Option[String] = {
    val vr = voteRequest().getBlockRange;
    if (block >= vr.getStartBlock && block < vr.getEndBlock) {
      Some(voteRequest().getMinerQueue(block - vr.getStartBlock)
        .getMinerCoaddr)
    } else {
      None
    }
  }

}