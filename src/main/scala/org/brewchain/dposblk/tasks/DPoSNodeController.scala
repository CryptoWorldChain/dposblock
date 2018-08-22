package org.brewchain.dposblk.tasks

import org.fc.brewchain.p22p.node.Network
import org.fc.brewchain.p22p.node.Node
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
import org.brewchain.dposblk.utils.DConfig
import org.brewchain.dposblk.pbgens.Dposblock.PBlockEntry
import org.brewchain.dposblk.pbgens.Dposblock.PBlockEntryOrBuilder
import org.brewchain.dposblk.pbgens.Dposblock.PSGetTransaction
import org.brewchain.dposblk.pbgens.Dposblock.PRetGetTransaction
import org.brewchain.dposblk.pbgens.Dposblock.PSGetTransaction
import org.brewchain.dposblk.pbgens.Dposblock.PRetGetTransaction
import onight.tfw.async.CallBack
import onight.tfw.otransio.api.beans.FramePacket
import org.brewchain.evmapi.gens.Tx.MultiTransaction
import org.brewchain.evmapi.gens.Block.BlockEntity
import org.brewchain.evmapi.gens.Block.BlockHeader
import org.brewchain.evmapi.gens.Block.BlockMiner

import scala.collection.JavaConversions._
import org.apache.commons.codec.binary.Base64
import org.apache.commons.codec.binary.Hex
import org.brewchain.account.util.ByteUtil
import java.util.concurrent.TimeUnit
import org.brewchain.bcapi.exec.SRunner
import org.fc.brewchain.p22p.action.PMNodeHelper

//投票决定当前的节点
case class DPosNodeController(network: Network) extends SRunner with PMNodeHelper with LogHelper {
  def getName() = "DCTRL"
  val DPOS_NODE_DB_KEY = "CURRENT_DPOS_KEY";
  val DPOS_NODE_DB_TERM = "CURRENT_DPOS_TERM";
  var cur_dnode: PDNode.Builder = PDNode.newBuilder()
  var term_Miner: PSDutyTermVote.Builder = PSDutyTermVote.newBuilder();
  var vote_Request: PSDutyTermVote.Builder = PSDutyTermVote.newBuilder();

  var isStop: Boolean = false;
  def updateVoteReq(pbo: PSDutyTermVote): Unit = {
    vote_Request = pbo.toBuilder()
    //    cur_dnode.setNodeCount(vote_Request.getCoNodes)
    //    syncToDB();
  }

  def saveVoteReq(pbo: PSDutyTermVote): Unit = {
    Daos.dposvotedb.put(
      "TERM-TEMP-" + pbo.getSign,
      OValue.newBuilder().setExtdata(pbo.toByteString()).build())
  }

  def loadVoteReq(sign: String): PSDutyTermVote.Builder = {
    val ov = Daos.dposvotedb.get("TERM-TEMP-" + sign).get
    if (ov != null) {
      PSDutyTermVote.newBuilder().mergeFrom(ov.getExtdata)
    } else {
      PSDutyTermVote.newBuilder()
    }
  }
  def loadNodeFromDB(): PDNode.Builder = {
    val ov = Daos.dpospropdb.get(DPOS_NODE_DB_KEY).get
    val root_node = network.root();
    if (ov == null) {
      cur_dnode.setBcuid(root_node.bcuid)
        .setCurBlock(1).setCoAddress(root_node.v_address)
        .setBitIdx(root_node.node_idx)
      Daos.dpospropdb.put(
        DPOS_NODE_DB_KEY,
        OValue.newBuilder().setExtdata(cur_dnode.build().toByteString()).build())
    } else {
      cur_dnode.mergeFrom(ov.getExtdata)
      if (!StringUtils.equals(cur_dnode.getBcuid, root_node.bcuid)) {
        log.warn("load from dnode info not equals with pzp node:" + cur_dnode + ",root=" + root_node)
        cur_dnode.setBcuid(root_node.bcuid);
        syncToDB();
      } else {
        log.info("load from db:OK:" + cur_dnode)
      }
    }

    if (cur_dnode.getCurBlock != Daos.actdb.getLastBlockNumber.intValue()) {
      log.warn("dpos block height Info not Equal to AccountDB:c=" +
        cur_dnode.getCurBlock + " ==> a=" + Daos.actdb.getLastBlockNumber);
      cur_dnode.setCurBlock(Daos.actdb.getLastBlockNumber.intValue())
      syncToDB()
    }

    val termov = Daos.dpospropdb.get(DPOS_NODE_DB_TERM).get
    if (termov == null) {
      Daos.dpospropdb.put(
        DPOS_NODE_DB_TERM,
        OValue.newBuilder().setExtdata(term_Miner.build().toByteString()).build())
    } else {
      if (DConfig.FORCE_RESET_VOTE_TERM == 1) {
        log.debug("force reset termreq:");
        Daos.dpospropdb.put(
          DPOS_NODE_DB_TERM,
          OValue.newBuilder().setExtdata(term_Miner.build().toByteString()).build())
      } else {
        term_Miner.mergeFrom(termov.getExtdata)
        log.debug("load from db:=" + DConfig.FORCE_RESET_VOTE_TERM);
      }
    }
    cur_dnode.setLastTermSign(term_Miner.getLastTermUid)
      .setTermId(term_Miner.getTermId)
      .setTermSign(term_Miner.getSign)
      .setTermStartBlock(term_Miner.getBlockRange.getStartBlock)
      .setTermEndBlock(term_Miner.getBlockRange.getEndBlock)
  }
  def syncToDB() {

    //    log.debug("checkMiner --> update cur_dnode::" + cur_dnode)
    Daos.dpospropdb.put(
      DPOS_NODE_DB_KEY,
      OValue.newBuilder().setExtdata(cur_dnode.build().toByteString()).build())
  }
  def updateTerm() = {
    cur_dnode.setDutyUid(term_Miner.getSign).setDutyStartMs(term_Miner.getTermStartMs)
      .setDutyEndMs(term_Miner.getTermEndMs)
    //    cur_dnode.setTermId(term_Miner.getTermId).setl;
    cur_dnode.setLastTermSign(term_Miner.getLastTermUid)
      .setTermId(term_Miner.getTermId)
      .setTermSign(term_Miner.getSign)
      .setTermStartBlock(term_Miner.getBlockRange.getStartBlock)
      .setTermEndBlock(term_Miner.getBlockRange.getEndBlock)

    Daos.dpospropdb.put(DPOS_NODE_DB_TERM,
      OValue.newBuilder().setExtdata(term_Miner.build().toByteString()).build())
    val lostM = Map[String, Node]();
    term_Miner.getMinerQueueList.map { mq =>
      if (DCtrl.coMinerByUID.filter(f => mq.getMinerCoaddr.equals(f._2.getCoAddress)).size == 0) {
        network.directNodes.map { dn =>
          if (dn.v_address.equals(mq.getMinerCoaddr)) {
            lostM.put(dn.bcuid, dn);
          }
        }
      }
    }
    if (lostM.size > 0) {
      hbTask.trySyncMinerInfo(lostM.values, network)
    }

  }
  def updateBlockHeight(blockHeight: Int) = {
    log.debug("checkMiner --> updateBlockHeight blockHeight::" + blockHeight + " cur_dnode.getCurBlock::" + cur_dnode.getCurBlock);
    if (blockHeight != cur_dnode.getCurBlock) {

      Daos.blkHelper.synchronized({
        //      if (cur_dnode.getCurBlock < blockHeight) {
        cur_dnode.setLastBlockTime(System.currentTimeMillis())
        //cur_dnode.setCurBlock(blockHeight)
        cur_dnode.setCurBlock(Daos.actdb.getLastBlockNumber.intValue());
        log.debug("checkMiner --> cur_dnode.setCurBlock::" + cur_dnode.getCurBlock);
        syncToDB()
        //      }
      })
    }
  }
  val hbTask = DTask_HeatBeat();
  def runOnce() = {
    Thread.currentThread().setName("DCTRL");
    implicit val _net = network
    MDCSetBCUID(network);
    MDCRemoveMessageID()
    var continue = true;
    while (continue && !isStop) {
      try {
        MDCSetMessageID(term_Miner.getSign)
        continue = false;
        log.info("DCTRL.RunOnce:S=" + cur_dnode.getState + ",B=" + cur_dnode.getCurBlock
          + ",CA=" + cur_dnode.getCoAddress
          + ",BCUID=" + term_Miner.getBcuid
          + ",MN=" + DCtrl.coMinerByUID.size
          + ",RN=" + network.bitenc.bits.bitCount
          + ",CN=" + term_Miner.getCoNodes
          + ",DU=" + cur_dnode.getDutyUid
          + ",VT=" + vote_Request.getTermId
          + ",TM=" + term_Miner.getTermId
          + ",TU=" + term_Miner.getSign
          + ",NextSec=" + JodaTimeHelper.secondFromNow(cur_dnode.getDutyEndMs)
          + ",SecPass=" + JodaTimeHelper.secondFromNow(cur_dnode.getLastDutyTime));
        cur_dnode.getState match {
          case DNodeState.DN_INIT =>
            //tell other I will join
            loadNodeFromDB();
            continue = DTask_CoMine.runOnce match {
              case n: PDNode if n == cur_dnode =>
                log.debug("dpos cominer init ok:" + n);
                Scheduler.scheduleWithFixedDelay(hbTask, 60, DConfig.HEATBEAT_TICK_SEC, TimeUnit.SECONDS);
                hbTask.onScheduled = true;
                true;
              case n: PDNode if !n.equals(cur_dnode) =>
                log.debug("dpos waiting for init:" + n);
                false
              case x @ _ =>
                log.debug("not ready:" + x);
                false
            }
          case DNodeState.DN_CO_MINER =>
            if (!hbTask.onScheduled) {
              Scheduler.scheduleWithFixedDelay(hbTask, 60, DConfig.HEATBEAT_TICK_SEC, TimeUnit.SECONDS);
              hbTask.onScheduled = true;
            }
            if (DTask_DutyTermVote.runOnce) {
              continue = true;
              cur_dnode.setState(DNodeState.DN_DUTY_MINER);
            } else {
              log.debug("cominer run false:" + cur_dnode.getCurBlock + ",vq[" + DCtrl.voteRequest().getBlockRange.getStartBlock
                + "," + DCtrl.voteRequest().getBlockRange.getEndBlock + "]" + ",vqid=" + DCtrl.voteRequest().getTermId
                + ",vqlid=" + DCtrl.voteRequest().getLastTermId + ",tid=" + term_Miner.getTermId
                + ",tq[" + term_Miner.getBlockRange.getStartBlock + "," + term_Miner.getBlockRange.getEndBlock + "]");
            }
          case DNodeState.DN_DUTY_MINER =>
            if (term_Miner.getBlockRange.getStartBlock > cur_dnode.getCurBlock + term_Miner.getMinerQueueCount) {
              log.debug("cur term force to resync block:" + cur_dnode.getCurBlock + ",vq[" + DCtrl.voteRequest().getBlockRange.getStartBlock
                + "," + DCtrl.voteRequest().getBlockRange.getEndBlock + "]" + ",vqid=" + DCtrl.voteRequest().getTermId
                + ",vqlid=" + DCtrl.voteRequest().getLastTermId + ",tid=" + term_Miner.getTermId
                + ",tq[" + term_Miner.getBlockRange.getStartBlock + "," + term_Miner.getBlockRange.getEndBlock + "]");
              continue = true;
              cur_dnode.setState(DNodeState.DN_SYNC_BLOCK);
            } else if (cur_dnode.getCurBlock >= term_Miner.getBlockRange.getEndBlock && term_Miner.getBlockRange.getEndBlock > 1 //|| DCtrl.voteRequest().getLastTermId >= term_Miner.getTermId
            ) {
              log.debug("cur term force to end:" + cur_dnode.getCurBlock + ",vq[" + DCtrl.voteRequest().getBlockRange.getStartBlock
                + "," + DCtrl.voteRequest().getBlockRange.getEndBlock + "]" + ",vqid=" + DCtrl.voteRequest().getTermId
                + ",vqlid=" + DCtrl.voteRequest().getLastTermId + ",tid=" + term_Miner.getTermId
                + ",tq[" + term_Miner.getBlockRange.getStartBlock + "," + term_Miner.getBlockRange.getEndBlock + "]");
              continue = true;
              cur_dnode.setState(DNodeState.DN_CO_MINER);
            } else if (DTask_MineBlock.runOnce) {
              if (cur_dnode.getCurBlock >= term_Miner.getBlockRange.getEndBlock
                || term_Miner.getTermId < vote_Request.getTermId) {
                val sleept = Math.abs((Math.random() * 100000000 % DConfig.DTV_TIME_MS_EACH_BLOCK).asInstanceOf[Long]) + 10;
                log.debug("cur term WILL end:newblk=" + cur_dnode.getCurBlock + ",term[" + DCtrl.voteRequest().getBlockRange.getStartBlock
                  + "," + DCtrl.voteRequest().getBlockRange.getEndBlock + "]" + ",T=" + term_Miner.getTermId + ",sleep=" + sleept);
                continue = true;
                cur_dnode.setState(DNodeState.DN_CO_MINER);
                DTask_DutyTermVote.synchronized({
                  DTask_DutyTermVote.wait(sleept)
                });
                true
              } else {
                //                log.debug("cur term NOT end:newblk=" + cur_dnode.getCurBlock + ",term[" + DCtrl.voteRequest().getBlockRange.getStartBlock
                //                  + "," + DCtrl.voteRequest().getBlockRange.getEndBlock + "]");
                false
              }
            } else {
              //check who mining.
              if (cur_dnode.getCurBlock >= term_Miner.getBlockRange.getEndBlock && term_Miner.getBlockRange.getEndBlock > 1) {
                continue = true;
                val sleept = Math.abs((Math.random() * 10000000 % DConfig.DTV_TIME_MS_EACH_BLOCK).asInstanceOf[Long]) + 10;
                cur_dnode.setState(DNodeState.DN_CO_MINER);
                //Thread.sleep(sleept);
                DTask_DutyTermVote.synchronized({
                  DTask_DutyTermVote.wait(sleept)
                });
                true
              } else {
                false;
              }
            }
          case DNodeState.DN_SYNC_BLOCK =>
            DTask_CoMine.runOnce
          case DNodeState.DN_BAKCUP =>
            DTask_CoMine.runOnce
          case _ =>
            log.warn("unknow State:" + cur_dnode.getState);

        }

      } catch {
        case e: Throwable =>
          log.warn("dpos control :Error", e);
      } finally {
        MDCRemoveMessageID()
      }
    }
  }
}

object DCtrl extends LogHelper {
  var instance: DPosNodeController = DPosNodeController(null);
  def dposNet(): Network = instance.network;
  //  val superMinerByUID: Map[String, PDNode] = Map.empty[String, PDNode];
  val coMinerByUID: Map[String, PDNode] = Map.empty[String, PDNode];
  def curDN(): PDNode.Builder = instance.cur_dnode
  def termMiner(): PSDutyTermVote.Builder = instance.term_Miner
  def voteRequest(): PSDutyTermVote.Builder = instance.vote_Request

  def getFastNode(): String = {
    var fastNode = curDN().build();
    coMinerByUID.map { f =>
      if (f._2.getCurBlock > fastNode.getCurBlock) {
        fastNode = f._2;
      }
    }
    fastNode.getBcuid
  }
  //  def curTermMiner(): PSDutyTermVoteOrBuilder = instance.term_Miner

  def isReady(): Boolean = {
    instance.network != null &&
      instance.cur_dnode.getStateValue > DNodeState.DN_INIT_VALUE
  }

  def checkMiner(block: Int, coaddr: String, mineTime: Long, threadName: String, maxWaitMS: Long = 1L): (Boolean, Boolean) = {
    val tm = termMiner().getBlockRange;
    val lastBlkTime = if (block == 1) termMiner().getTermStartMs else
      Math.max(Daos.blkHelper.GetBestBlock().getHeader.getTimestamp, termMiner().getTermStartMs);
    val blkshouldMineMS = tm.getEachBlockMs + lastBlkTime

    log.debug("checkMiner --> block::" + block + " curDN.getCurBlock::" + curDN.getCurBlock + " tm.getEndBlock::" + tm.getEndBlock + " tm.getStartBlock::" + tm.getStartBlock + " coaddr::" + coaddr)
    if (block > tm.getEndBlock || block < tm.getStartBlock) {
      log.debug("checkMiner:False,block too large:" + block + ",[" + tm.getStartBlock + "," + tm.getEndBlock + "],sign="
        + termMiner.getSign + ",TID=" + termMiner.getTermId)
      val maxblk = Math.max(block, tm.getEndBlock)
      log.debug("checkMiner --> maxblk::" + maxblk + " curDN.getCurBlock::" + curDN.getCurBlock)
      if (maxblk > curDN.getCurBlock && System.currentTimeMillis() > blkshouldMineMS + DConfig.BLK_EPOCH_MS) {
        val fastuid = DCtrl.getFastNode();
        if (!StringUtils.equals(fastuid, curDN.getBcuid)) {
          BlockSync.tryBackgroundSyncLogs(maxblk, fastuid)(DCtrl.dposNet())
        }
      }
      (false, false)
    } else {
      val realblkMineMS = mineTime;
      val termblockLeft = block - tm.getEndBlock
      minerByBlockHeight(block) match {
        case Some(n) =>
          if (coaddr.equals(n)) {
            //if (DCtrl.termMiner().getMinerQueue(101-DCtrl.termMiner().getBlockRange.getStartBlock).getBlockHeight == block + 1) {
            if (realblkMineMS < blkshouldMineMS) {
              log.debug("wait for time to Mine:Should=" + blkshouldMineMS + ",realblkminesec=" + realblkMineMS + ",eachBlockMS=" + tm.getEachBlockMs + ",TermLeft=" + termblockLeft
                + ",TID=" + termMiner().getTermId + ",TS=" + termMiner().getSign + ",bh=" + block);
              Thread.sleep(Math.min(maxWaitMS, blkshouldMineMS - realblkMineMS));
            }
            log.debug("checkMiner --> realblkMineMS::" + realblkMineMS + " blkshouldMineMS::" + blkshouldMineMS + " n::" + n + " coaddr::" + coaddr)
            (true, false)
            //            } else {
            //              log.debug("checkMiner --> current height::" + block + " want height::" + DCtrl.termMiner().getMinerQueue(101-DCtrl.termMiner().getBlockRange.getStartBlock).getBlockHeight)
            //              (false, false)
            //            }
          } else {
            if (block >= 1 && realblkMineMS > blkshouldMineMS + DConfig.MAX_WAIT_BLK_EPOCH_MS) {
              minerByBlockHeight(block + ((realblkMineMS - blkshouldMineMS) / DConfig.MAX_WAIT_BLK_EPOCH_MS).asInstanceOf[Int]) match {
                case Some(nn) =>
                  log.debug("Override miner for Next:check:" + blkshouldMineMS + ",realblkmine=" + realblkMineMS + ",n=" + n
                    + ",next=" + nn + ",coaddr=" + coaddr + ",block=" + (block) + ",TermLeft=" + termblockLeft + ",Result=" + coaddr.equals(nn)
                    + ",TID=" + termMiner().getTermId + ",TS=" + termMiner().getSign); //try to revote.
                  (coaddr.equals(nn), true)
                case None =>
                  log.debug("wait for Miner:Should=" + blkshouldMineMS + ",Real=" + realblkMineMS + ",eachBlockMS=" + tm.getEachBlockMs + ",TermLeft=" + termblockLeft
                    + ",TID=" + termMiner().getTermId + ",TS=" + termMiner().getSign);
                  (false, true)
              }
            } else {

              log.debug("wait for timeout to Mine:ShouldT=" + (blkshouldMineMS + DConfig.MAX_WAIT_BLK_EPOCH_MS) + ",realblkmine=" + realblkMineMS + ",eachBlockSec=" + tm.getEachBlockMs
                + ",TermLeft=" + termblockLeft);
              if (realblkMineMS < blkshouldMineMS) {
                Thread.sleep(Math.min(maxWaitMS, blkshouldMineMS - realblkMineMS));
              } else {
                //request block if not sync
                if (System.currentTimeMillis() > blkshouldMineMS + DConfig.BLK_EPOCH_MS) {
                  var bestfastUID = ""
                  coMinerByUID.map { f =>
                    if (f._2.getCoAddress.equals(n)) {
                      bestfastUID = f._2.getBcuid;
                    }
                  }
                  BlockSync.tryBackgroundSyncLogs(block, bestfastUID)(dposNet());
                }
              }
              (false, false)
            }

          }
        case None =>
          if (maxWaitMS >= 1 && realblkMineMS < blkshouldMineMS) {
            //            log.debug("wait for time to Mine:Should=" + blkshouldMineMS + ",realblkminesec=" + realblkMineMS + ",eachBlockSec=" + tm.getEachBlockSec + ",TermLeft=" + termblockLeft);
            Thread.sleep(Math.min(maxWaitMS, blkshouldMineMS - realblkMineMS));
          }
          (false, false)
      }
    }
  }
  def minerByBlockHeight(block: Int): Option[String] = {
    val tm = termMiner().getBlockRange;
    if (block >= tm.getStartBlock && block <= tm.getEndBlock) {
      Some(termMiner().getMinerQueue(block - tm.getStartBlock)
        .getMinerCoaddr)
    } else if (block > tm.getStartBlock && termMiner().getMinerQueueCount > 0) {
      Some(termMiner().getMinerQueue((block - tm.getStartBlock)
        % termMiner().getMinerQueueCount)
        .getMinerCoaddr)
    } else {
      None
    }
  }
  def createNewBlock(txc: Int): BlockEntity.Builder = {
    Daos.blkHelper.synchronized({
      val newblk = Daos.blkHelper.CreateNewBlock(DCtrl.termMiner().getMaxTnxEachBlock, //只是打块！其中某些成功广播的tx，默认是80%
        (DCtrl.coMinerByUID.size * DConfig.CREATE_BLOCK_TX_CONFIRM_PERCENT / 100).asInstanceOf[Int],
        "");
      val newblockheight = curDN().getCurBlock + 1
      if (newblk == null || newblk.getHeader == null) {
        log.debug("new block header is null: ch=" + newblockheight + ",dbh=" + newblk);
        null
      } else if (newblockheight != newblk.getHeader.getNumber) {
        log.debug("mining error: ch=" + newblockheight + ",dbh=" + newblk.getHeader.getNumber);
        null
      } else {
        newblk
      }
    })

  }
  def saveBlock(b: PBlockEntryOrBuilder): (Int, Int) = {
    Daos.blkHelper.synchronized({
      if (!b.getCoinbaseBcuid.equals(DCtrl.curDN().getBcuid)) {
        val res = Daos.blkHelper.ApplyBlock(b.getBlockHeader);

        if (res.getTxHashsCount > 0) {
          log.debug("must sync transaction first.");
          val reqTx = PSGetTransaction.newBuilder();
          for (txHash <- res.getTxHashsList) {
            reqTx.addTxHash(txHash);
          }
          val miner = BlockEntity.parseFrom(b.getBlockHeader);

          // DCtrl.coMinerByUID.find(p => { p._2.getCurBlock > curDN().getCurBlock) {
          // val randomNode = dposNet().directNodes.filter { x => !x.bcuid.equals(DCtrl.curDN().getBcuid ) }.toList
          var oNetwork = dposNet().directNodeByBcuid.get(miner.getMiner.getBcuid)
          if (!oNetwork.nonEmpty) {
            log.warn("not found block minner")
            val blockNode = DCtrl.coMinerByUID.filter(p => p._2.getCurBlock > curDN().getCurBlock).toList
            if (blockNode.size != 0) {
              oNetwork = dposNet().directNodeByBcuid.get(blockNode.get(0)._2.getBcuid);
            } else {
              log.warn("random block minner")
              val randomNode = dposNet().directNodes.filter { x => !x.bcuid.equals(DCtrl.curDN().getBcuid) }.toList
              oNetwork = Option(randomNode.get(scala.util.Random.nextInt(randomNode.size)));
            }
          }
          // .getOrElse(randomNode.get(scala.util.Random.nextInt(randomNode.size)))
          dposNet().asendMessage("SRTDOB", reqTx.build(), oNetwork.get, new CallBack[FramePacket] {
            def onSuccess(fp: FramePacket) = {
              try {
                val retTx = if (fp.getBody != null) {
                  PRetGetTransaction.newBuilder().mergeFrom(fp.getBody);
                } else {
                  log.warn("not found sync transaction");
                  null;
                }
                if (retTx != null) {
                  for (x <- retTx.getTxContentList) {
                    Daos.txHelper.syncTransaction(MultiTransaction.parseFrom(x).toBuilder(), false);
                  }
                  log.debug("sync transaction all done total::" + retTx.getTxContentList.size());
                  Daos.blkHelper.ApplyBlock(b.getBlockHeader)
                }
              } finally {
              }
            }
            def onFailed(e: java.lang.Exception, fp: FramePacket) {
              log.debug("sync transaction error::" + e.getMessage, e)
            }
          }, '8')
          //} 
          //})
        }
        if (res.getCurrentNumber > 0) {
          log.debug("checkMiner --> updateBlockHeight::" + res.getCurrentNumber.intValue())
          DCtrl.instance.updateBlockHeight(res.getCurrentNumber.intValue())
          (res.getCurrentNumber.intValue(), res.getWantNumber.intValue())
        } else {
          (res.getCurrentNumber.intValue(), res.getWantNumber.intValue())
        }
      } else {
        log.debug("checkMiner --> updateBlockHeight::" + b.getBlockHeight)
        DCtrl.instance.updateBlockHeight(b.getBlockHeight)
        (b.getBlockHeight, b.getBlockHeight)
      }
    }) //synchronized
  }

  def loadFromBlock(block: Int): PBlockEntry.Builder = {
    //    val ov = Daos.dposdb.get("D" + block).get
    //    if (ov != null) {
    val blk = Daos.actdb.getBlockByNumber(block);
    if (blk != null) {
      val b = PBlockEntry.newBuilder().setBlockHeader(blk.toBuilder().clearBody().build().toByteString()).setBlockHeight(block)
      //          .setBlockHeight(block)
      //          .setSign(Hex.encodeHexString(blk.getHeader.getBlockHash.toByteArray()))
      //          .setSliceId(blk.getHeader.getSliceId.asInstanceOf[Int])
      //          .setCoinbaseBcuid(blk.getMiner.getAddress)
      //      log.debug("load block ok =" + block + ",S=" + blk.getHeader.getSliceId + ",CB=" + blk.getMiner.getBcuid
      //        + ",sign=" + blk.getHeader.getBlockHash)
      b
    } else {
      log.debug("blk not found in AccountDB:" + block);
      null;
    }

    //    } else {
    //      log.debug("blk not found in DPosDB:" + block);
    //      null
    //    }

  }

}