import java.util.Comparator;
import java.util.Date;

public class RemotePeerInfo implements Comparator<RemotePeerInfo>
{
    public final boolean comparator=false;
    public String peerId;
    public String peerAddress;
    public String peerPort;
    public int isFirstPeer;
    public double streamRate = 0;
    public int isInterested = 1;
    public int isPreferredNeighbor = 0;
    public int isOptUnchokedNeighbor = 0;
    public int isChoked = 1;
    public PayLoadData payloadData;
    public int state = -1;
    public int peerIndex;
    public int isCompleted = 0;
    public int isHandShake = 0;
    public Date sTime;
    public Date fTime;

    public RemotePeerInfo() {

    }

    public String getPeerId() {
        return peerId;
    }

    public void setPeerId(String peerId) {
        this.peerId = peerId;
    }

    public String getPeerAddress() {
        return peerAddress;
    }

    public void setPeerAddress(String peerAddress) {
        this.peerAddress = peerAddress;
    }

    public String getPeerPort() {
        return peerPort;
    }

    public void setPeerPort(String peerPort) {
        this.peerPort = peerPort;
    }

    public boolean isHasFile() {
        return hasFile;
    }

    public void setHasFile(boolean hasFile) {
        this.hasFile = hasFile;
    }

    public int getPeerPos() {
        return peerPos;
    }

    public void setPeerPos(int peerPos) {
        this.peerPos = peerPos;
    }


    public boolean hasFile;
    public int peerPos;
    public boolean isFirst;
    public RemotePeerInfo(String pId, String pAddress, String pPort,boolean hFile) {
        peerId = pId;
        peerAddress = pAddress;
        peerPort = pPort;
        hasFile=hFile;
        payloadData =new PayLoadData();
    }
    public int compareTo(RemotePeerInfo remotePeerInfo)
    {
        return Double.compare(this.streamRate,remotePeerInfo.streamRate);
    }
    public int compare(RemotePeerInfo p1,RemotePeerInfo p2)
    {
        if(p1==null && p2==null)
            return 0;
        if(p1==null)
        {
            return 1;
        }
        if(p2==null)
        {
            return -1;
        }
        if(comparator)
        {
            return p1.compareTo(p2);
        }
        else
        {
            return p2.compareTo(p1);
        }
    }

}