package org.brewchain.dposblk.action


import org.apache.felix.ipojo.annotations.Instantiate
import org.apache.felix.ipojo.annotations.Provides
import onight.tfw.ntrans.api.ActorService
import onight.tfw.proxy.IActor
import onight.tfw.otransio.api.session.CMDService
import onight.osgi.annotation.NActorProvider
import org.brewchain.dposblk.PSMDPoSNet
import org.fc.brewchain.p22p.utils.LogHelper
import onight.oapi.scala.commons.PBUtils
import onight.oapi.scala.commons.LService
import org.fc.brewchain.p22p.action.PMNodeHelper
import onight.tfw.otransio.api.beans.FramePacket
import onight.tfw.async.CompleteHandler
import org.brewchain.bcapi.utils.PacketIMHelper._
import org.brewchain.dposblk.pbgens.Dposblock.PSCoMine
import org.brewchain.dposblk.pbgens.Dposblock.PRetCoMine
import org.brewchain.dposblk.pbgens.Dposblock.PCommand
import org.brewchain.dposblk.pbgens.Dposblock.PSSyncBlocks
import org.brewchain.dposblk.pbgens.Dposblock.PRetSyncBlocks

@NActorProvider
@Instantiate
@Provides(specifications = Array(classOf[ActorService], classOf[IActor], classOf[CMDService]))
class PDPoSBlockSync extends PSMDPoSNet[PSSyncBlocks] {
  override def service = PDPoSBlockSyncService
}

//
// http://localhost:8000/fbs/xdn/pbget.do?bd=
object PDPoSBlockSyncService extends LogHelper with PBUtils with LService[PSSyncBlocks] with PMNodeHelper {
  override def onPBPacket(pack: FramePacket, pbo: PSSyncBlocks, handler: CompleteHandler) = {
    log.debug("PDPoSBetTermsService::" + pack.getFrom())
    var ret = PRetSyncBlocks.newBuilder();
//    if (!DPOS.isReady()) {
//      ret.setRetCode(-1).setRetMessage("Raft Network Not READY")
//      handler.onFinished(PacketHelper.toPBReturn(pack, ret.build()))
//    } else {
//      try {
//        MDCSetBCUID(RSM.raftNet)
//        MDCSetMessageID(pbo.getMessageId)
//        ret.setMessageId(pbo.getMessageId);
//        //
//        ret.setRn(RSM.curRN());
//        RSM.raftFollowNetByUID.map(rn => {
//          ret.addNodes(rn._2);
//        })
//        //        if (pbo.getRn.getState == RaftState.RS_INIT) {
//        RSM.raftFollowNetByUID.put(pbo.getRn.getBcuid, pbo.getRn);
//        //        }
//        ret.setRetCode(0).setRetMessage("SUCCESS");
//      } catch {
//        case e: FBSException => {
//          ret.clear()
//          ret.setRetCode(-2).setRetMessage(e.getMessage)
//        }
//        case t: Throwable => {
//          log.error("error:", t);
//          ret.clear()
//          ret.setRetCode(-3).setRetMessage(t.getMessage)
//        }
//      } finally {
//        handler.onFinished(PacketHelper.toPBReturn(pack, ret.build()))
//      }
//    }
  }
  //  override def getCmds(): Array[String] = Array(PWCommand.LST.name())
  override def cmd: String = PCommand.SYN.name();
}
