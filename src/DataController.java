import java.io.*;
import java.net.Socket;
import java.util.*;


public class DataController implements Runnable
{
    private static String pId = null;
    RandomAccessFile rf;

    public DataController(String pId) {
        DataController.pId = pId;
    }

    public void run() {
        MessageData m;
        DataParams dp;
        String dataType;
        String currentPeerId;

        while(true)
        {
            dp  = P2P.removeDataFromQueue();
            while(dp == null) {
                Thread.currentThread();
                try {
                    Thread.sleep(500);
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
                dp  = P2P.removeDataFromQueue();
            }

            m = dp.getM();

            dataType = m.getDataType();
            currentPeerId = dp.getpId();
            int state = P2P.remotePeerInfoHashMap.get(currentPeerId).state;
            if(dataType.equals(""+Constants.have) && state != 14)
            {
                P2P.l.showLog(P2P.peerId+" got HAVE message from Peer "+ currentPeerId);
                if(comparePayLoadData(currentPeerId,m)) {
                    sendInterestedMessage(currentPeerId,P2P.peerData.get(currentPeerId));
                    P2P.remotePeerInfoHashMap.get(currentPeerId).state = 9;
                }
                else {
                    sendNotInterestedMessage(currentPeerId,P2P.peerData.get(currentPeerId));
                    P2P.remotePeerInfoHashMap.get(currentPeerId).state = 13;
                }
            }
            else {
                switch (state)
                {
                    case 2:
                        if (dataType.equals(""+Constants.bitField)) {
                            P2P.l.showLog(P2P.peerId+" got BITFIELD message from Peer "+ currentPeerId);
                            sendBitFieldMessage(currentPeerId,P2P.peerData.get(currentPeerId));
                            P2P.remotePeerInfoHashMap.get(currentPeerId).state = 3;
                        }
                        break;

                    case 3:
                        
                         if (dataType.equals(""+Constants.intersted)) {
                            P2P.l.showLog(P2P.peerId+" got a REQUEST message to Peer "+ currentPeerId);
                            P2P.l.showLog( P2P.peerId+" got INTERESTED message from Peer "+currentPeerId);
                            P2P.remotePeerInfoHashMap.get(currentPeerId).isInterested = 1;
                            P2P.remotePeerInfoHashMap.get(currentPeerId).isHandShake = 1;

                            if(!P2P.preferredNeighboursHashMapTable.containsKey(currentPeerId) && !P2P.unchokedNeighboursHashMapTable.containsKey(currentPeerId)) {
                                sendChokeMessage(currentPeerId,P2P.peerData.get(currentPeerId));
                                P2P.remotePeerInfoHashMap.get(currentPeerId).isChoked = 1;
                                P2P.remotePeerInfoHashMap.get(currentPeerId).state  = 6;
                            }
                            else {
                                P2P.remotePeerInfoHashMap.get(currentPeerId).isChoked = 0;
                                sendUnChokeMessage(currentPeerId,P2P.peerData.get(currentPeerId));
                                P2P.remotePeerInfoHashMap.get(currentPeerId).state = 4 ;
                            }
                        }
                        else if (dataType.equals(""+Constants.notInterested)) {
                            P2P.l.showLog( P2P.peerId+" got NOT INTERESTED message from Peer "+currentPeerId);
                            P2P.remotePeerInfoHashMap.get(currentPeerId).isInterested = 0;
                            P2P.remotePeerInfoHashMap.get(currentPeerId).state = 5;
                            P2P.remotePeerInfoHashMap.get(currentPeerId).isHandShake = 1;
                        }
                        break;

                    case 4:
                        if (dataType.equals(""+Constants.request)) {
                            dataTransfer(P2P.peerData.get(currentPeerId), m, currentPeerId);
                            if(!P2P.preferredNeighboursHashMapTable.containsKey(currentPeerId) && !P2P.unchokedNeighboursHashMapTable.containsKey(currentPeerId)) {
                                sendChokeMessage(currentPeerId,P2P.peerData.get(currentPeerId));
                                P2P.remotePeerInfoHashMap.get(currentPeerId).isChoked = 1;
                                P2P.remotePeerInfoHashMap.get(currentPeerId).state = 6;
                            }
                        }
                        break;

                    case 8:
                        if (dataType.equals(""+Constants.bitField)) {
                            if(comparePayLoadData(currentPeerId,m)) {
                                sendInterestedMessage(currentPeerId,P2P.peerData.get(currentPeerId));
                                P2P.remotePeerInfoHashMap.get(currentPeerId).state = 9;
                            }
                            else {
                                sendNotInterestedMessage(currentPeerId,P2P.peerData.get(currentPeerId));
                                P2P.remotePeerInfoHashMap.get(currentPeerId).state = 13;
                            }
                        }
                        break;

                    case 9:
                        if (dataType.equals(""+Constants.choke)) {
                            P2P.l.showLog( P2P.peerId+" got CHOKED by Peer "+currentPeerId);
                            P2P.remotePeerInfoHashMap.get(currentPeerId).state = 14;
                        }
                        else if (dataType.equals(""+Constants.unChoke)) {
                            P2P.l.showLog( P2P.peerId+" got CHOKED by Peer "+currentPeerId);
                            try {
                                Thread.sleep(5000);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                            P2P.l.showLog( P2P.peerId+" got UNCHOKED by Peer "+currentPeerId);
                            int initialMismatch = P2P.currentDataPayLoad.fetchFirstBitField(
                                    P2P.remotePeerInfoHashMap.get(currentPeerId).payloadData);
                            if(initialMismatch != -1) {
                                sendRequest(initialMismatch,P2P.peerData.get(currentPeerId));
                                P2P.remotePeerInfoHashMap.get(currentPeerId).state = 11;
                                P2P.remotePeerInfoHashMap.get(currentPeerId).sTime = new Date();
                            }
                            else
                                P2P.remotePeerInfoHashMap.get(currentPeerId).state = 13;
                        }
                        break;

                    case 11:
                        if (dataType.equals(""+Constants.choke)) {
                        P2P.l.showLog( P2P.peerId+" got CHOKED by Peer "+currentPeerId);
                        P2P.remotePeerInfoHashMap.get(currentPeerId).state = 14;
                        }
                        
                        else if (dataType.equals(""+Constants.piece)) {
                            byte[] payloadArray = m.getPayLoadArray();
                            P2P.remotePeerInfoHashMap.get(currentPeerId).fTime = new Date();
                            long d = P2P.remotePeerInfoHashMap.get(currentPeerId).fTime.getTime() - P2P.remotePeerInfoHashMap.get(currentPeerId).sTime.getTime() ;
                            P2P.remotePeerInfoHashMap.get(currentPeerId).streamRate= ((double)(payloadArray.length + Constants.sizeOfMessage + Constants.typeOfMessage) / (double)d) * 100;
                            Payloadpiece p = Payloadpiece.convertToPiece(payloadArray);
                            P2P.currentDataPayLoad.updatePayLoad(p,""+currentPeerId);
                            int indx = P2P.currentDataPayLoad.fetchFirstBitField(
                                    P2P.remotePeerInfoHashMap.get(currentPeerId).payloadData);
                            if(indx != -1) {
                                sendRequest(indx,P2P.peerData.get(currentPeerId));
                                P2P.remotePeerInfoHashMap.get(currentPeerId).state  = 11;
                                P2P.remotePeerInfoHashMap.get(currentPeerId).sTime = new Date();
                            }
                            else
                                P2P.remotePeerInfoHashMap.get(currentPeerId).state = 13;
                            P2P.readNextPeerData();;

                            Enumeration<String> keys = Collections.enumeration(P2P.remotePeerInfoHashMap.keySet());
                            while(keys.hasMoreElements())
                            {
                                String nextElement = keys.nextElement();
                                RemotePeerInfo r = P2P.remotePeerInfoHashMap.get(nextElement);
                                if(nextElement.equals(P2P.peerId))continue;
                                if (r.isCompleted == 0 && r.isChoked == 0 && r.isHandShake == 1) {
                                    sendHaveMessage(nextElement,P2P.peerData.get(nextElement));
                                    P2P.remotePeerInfoHashMap.get(nextElement).state = 3;
                                }
                            }
                        }
                         
                        break;

                    case 14:
                        if (dataType.equals(""+Constants.unChoke)) {
                            P2P.l.showLog( P2P.peerId+" got CHOKED by Peer "+currentPeerId);
                            try {
                            Thread.sleep(6000);
                            } catch (Exception e) {
                           System.out.println(e.getMessage());;
                            }
                            P2P.l.showLog( P2P.peerId+" got UNCHOKED by Peer "+currentPeerId);
                            P2P.remotePeerInfoHashMap.get(currentPeerId).state = 14;
                        }
                        else if (dataType.equals(""+Constants.have)) {
                            if(comparePayLoadData(currentPeerId,m)) {
                                sendInterestedMessage(currentPeerId,P2P.peerData.get(currentPeerId));
                                P2P.remotePeerInfoHashMap.get(currentPeerId).state = 9;
                            }
                            else {
                                sendNotInterestedMessage(currentPeerId,P2P.peerData.get(currentPeerId) );
                                P2P.remotePeerInfoHashMap.get(currentPeerId).state = 13;
                            }
                        }
                        
                        break;
                }
            }

        }
    }

    

    private void dataTransfer(Socket socket, MessageData requestMessage, String pId)
    {
        byte[] bindx = requestMessage.getPayLoadArray();
        int pindx = Constants.convertByteArrayToInt(bindx,0);
        byte[] byteRead = new byte[Constants.pieceSize];
        int readBytes = 0;
        File f = new File(P2P.peerId, Constants.fileName);

        P2P.l.showLog(P2P.peerId+" is sending PIECE "+pindx+" to Peer "+ pId);
        try {
            rf = new RandomAccessFile(f,"r");
            rf.seek((long) pindx *Constants.pieceSize);
            readBytes = rf.read(byteRead, 0, Constants.pieceSize);
        }
        catch (Exception ex) {
            P2P.l.showLog(P2P.peerId+" has error in reading the file: "+ex.toString());
        }

        byte[] buffbytes = new byte[readBytes + Constants.maxPieceLength];
        System.arraycopy(bindx, 0, buffbytes, 0, Constants.maxPieceLength);
        System.arraycopy(byteRead, 0, buffbytes, Constants.maxPieceLength, readBytes);

        sendOutput(MessageData.convertDataToByteArray(new MessageData(Constants.piece, buffbytes)), socket);
        try{rf.close();}
        catch(Exception ignored){}
    }
    private void sendRequest( int pNo,Socket socket) {
        byte[] p = new byte[Constants.maxPieceLength];
        for (int i = 0; i < Constants.maxPieceLength; i++)
            p[i] = 0;

        byte[] pindxArray = Constants.convertIntToByte(pNo);
        System.arraycopy(pindxArray, 0, p, 0,
                pindxArray.length);
       
        sendOutput(MessageData.convertDataToByteArray(new MessageData(Constants.request, p)),socket);
    }
   

    private void sendNotInterestedMessage(String pId,Socket socket ) {
        P2P.l.showLog(P2P.peerId+" sent a NOT INTERESTED message to Peer "+ pId);
               sendOutput(MessageData.convertDataToByteArray(new MessageData(Constants.notInterested)),socket);
    }

    private void sendInterestedMessage(String pId,Socket socket) {
        P2P.l.showLog(P2P.peerId+" sent a REQUEST message to Peer "+ pId);
        P2P.l.showLog(P2P.peerId+" sent a INTERESTED message to Peer "+ pId);
        sendOutput(MessageData.convertDataToByteArray(new MessageData(Constants.intersted)),socket);
    }
    private boolean comparePayLoadData( String pId,MessageData md) {
        PayLoadData payloadData = PayLoadData.decodeData(md.getPayLoadArray());
        P2P.remotePeerInfoHashMap.get(pId).payloadData = payloadData;
        return P2P.currentDataPayLoad.comparePayLoadData(payloadData);
    }
    private void sendUnChokeMessage( String pId,Socket socket) {
        P2P.l.showLog(P2P.peerId+" sent a UNCHOKE message to Peer "+ pId);
        sendOutput(MessageData.convertDataToByteArray(new MessageData(Constants.unChoke)),socket);
    }


    private void sendChokeMessage(String pId,Socket socket) {
        P2P.l.showLog(P2P.peerId+" sent a CHOKE message to Peer "+ pId);
        sendOutput(MessageData.convertDataToByteArray(new MessageData(Constants.choke)),socket);
    }
    private void sendOutput(byte[] encodedBitField,Socket socket ) {
        try {
            OutputStream op = socket.getOutputStream();
            op.write(encodedBitField);
        } catch (Exception ex) {
            System.out.println(ex.getMessage());;
        }
    }
    private void sendHaveMessage(String pId,Socket socket)  {
        P2P.l.showLog(P2P.peerId+" sent a HAVE message to Peer "+ pId);
        sendOutput( MessageData.convertDataToByteArray(new MessageData(Constants.have, P2P.currentDataPayLoad.encodeData())),socket);
    }

      
   
    private void sendBitFieldMessage(String pId,Socket socket) {
        P2P.l.showLog(P2P.peerId+" sent a BITFIELD message to Peer "+ pId);
        sendOutput(MessageData.convertDataToByteArray(new MessageData(+Constants.bitField, P2P.currentDataPayLoad.encodeData())),socket);
    }


}
