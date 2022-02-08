import java.io.*;

public class PayLoadData
{
    public Payloadpiece[] pieceData;
    public int bSize;
    public static Logger l;
    PayLoadData()
    {
        double d=(double)Constants.fileSize/Constants.pieceSize;
        this.bSize=(int)Math.ceil(d);
        this.pieceData=new Payloadpiece[this.bSize];
        int i=0;
        while(i<this.bSize)
        {
            this.pieceData[i++]=new Payloadpiece();
        }
    }
    public int getbSize() {
        return bSize;
    }

    public void setbSize(int bSize) {
        this.bSize = bSize;
    }
    public Payloadpiece[] getPieceData() {
        return pieceData;
    }

    public void setPieceData(Payloadpiece[] pieceData) {
        this.pieceData = pieceData;
    }
    public synchronized boolean comparePayLoadData(PayLoadData p)
    {
        int csize=p.getbSize();
        int i=0;
        while(i<csize)
        {
            if(p.getPieceData()[i].getHasPiece()==1 && this.getPieceData()[i].getHasPiece()==0)
            {
                return true;
            }
            i++;
        }
        return false;
    }
    public synchronized int fetchFirstBitField(PayLoadData p)
    {
        if(this.getbSize()>=p.getbSize())
        {
            int i=0;
            while(i<p.getbSize())
            {
                if(p.getPieceData()[i].getHasPiece()==1 && this.getPieceData()[i].getHasPiece()==0)
                {
                    return i;
                }
                i++;
            }
        }
        else {
            int i = 0;
            while (i < this.getbSize())
            {
                if (p.getPieceData()[i].getHasPiece() == 1 && this.getPieceData()[i].getHasPiece() == 0)
                {
                    return i;
                }
                i++;
            }
        }
        return -1;
    }
    public byte[] encodeData()
    {
        int s=0;
        if(this.bSize%8!=0)
        {
            s+=1;
        }
         s+=this.bSize/8;
        byte[] bArray=new byte[s];
        int temp=0;
        int bindx=0;
        int i=1;
        while(i<=this.bSize)
        {
            int t1=this.pieceData[i-1].hasPiece;
            temp=temp<<1;
            if(t1==1)
            {
                temp++;
            }
            if(i%8==0)
            {
                bArray[bindx]=(byte) temp;
                bindx++;
                temp=0;
            }
            i++;
        }
        i--;
        if(i%8!=0)
        {
            int shift=this.bSize-(this.bSize/8)*8;
            temp<<=(8-shift);
            bArray[bindx]=(byte)temp;
        }
        return bArray;
    }
    public static PayLoadData decodeData(byte[] b)
    {
       PayLoadData p=new PayLoadData();
       int i=0;
       while(i<b.length)
       {
           int c=7;
           while(c>=0)
           {
             int pNo=1<<c;
             int k=i*8+(8-c-1);
             if(k<p.bSize)
             {

                 if((b[i]&(pNo))!=0)
                 {
                    p.pieceData[k].hasPiece=1;
                 }
                 else
                 {
                     p.pieceData[k].hasPiece=0;
                 }
             }
             c--;
           }
           i++;
       }
       return p;
    }
    public boolean hasAllPieces()
    {

        for(int i=0;i<this.bSize;i++)
        {
            if(this.pieceData[i].hasPiece==0)
            {
                return false;
            }
        }
        return true;

    }


    public void initPayLoad(String pId, boolean hasFile)
    {
        int i=0;
        if(hasFile)
        {
            while(i<bSize)
            {
                this.pieceData[i].setHasPiece(1);
                this.pieceData[i].setSenderpId(pId);
                i++;
            }

        }
        else
        {
            while(i<bSize)
            {
                this.pieceData[i].setHasPiece(0);
                this.pieceData[i].setSenderpId(pId);
                i++;
            }
        }

    }
    public int avaliablePieces()
    {
        int pc=0;
        for(int i=0;i<this.bSize;i++)
        {
            if(this.pieceData[i].hasPiece==1)
            {
                pc+=1;
            }
        }
        return pc;
    }
    public synchronized  void updatePayLoad(Payloadpiece p, String pId )  {
        if(P2P.currentDataPayLoad.pieceData[p.pindx].hasPiece==1)
        {
            P2P.l.showLog(pId+" This piece already exists");
        }
        else
        {
            try {
                byte[] writeData;
                int offset = p.pindx * Constants.pieceSize;
                File f = new File(P2P.peerId, Constants.fileName);
                RandomAccessFile r = new RandomAccessFile(f, "rw");
                writeData = p.piece;
                r.seek(offset);
                r.write(writeData);
                r.close();
                this.pieceData[p.pindx].setHasPiece(1);
                this.pieceData[p.pindx].setSenderpId(pId);
                P2P.l.showLog(
                        P2P.peerId + " Peer has downloaded the piece " + p.pindx + " from peer " + pId + ". It now contains " + P2P.currentDataPayLoad.avaliablePieces() + " pieces");
                if (P2P.currentDataPayLoad.hasAllPieces()) {
                    P2P.remotePeerInfoHashMap.get(P2P.peerId).isInterested = 0;
                    P2P.remotePeerInfoHashMap.get(P2P.peerId).isCompleted = 1;
                    P2P.remotePeerInfoHashMap.get(P2P.peerId).isChoked = 0;
                    updatePeerConfig(P2P.peerId);
                    P2P.l.showLog(P2P.peerId + " has completed downloading the file!!!");
                    P2P.l.showLog(P2P.peerId + " is sending NOT INTERESTED MESSAGE");
                }
            }
            catch (Exception ex)
            {
                P2P.l.showLog(ex.getMessage());
            }
        }
    }
    public void updatePeerConfig(String pId)
    {
        String str="";
        String l;
        BufferedReader br;
        BufferedWriter bw;
        try
        {
             br=new BufferedReader(new FileReader(Constants.PEERS_PATH));
            while((l=br.readLine())!=null)
            {
               String[] st=l.trim().split(" ");
                if(st[0].equals(pId))
                {
                    st[3]="1";
                    l=st[0]+" "+st[1]+" "+st[2]+" "+st[3];
                }
                str+=l+"\n";
            }
            br.close();
            bw=new BufferedWriter(new FileWriter(Constants.PEERS_PATH));
            bw.write(str);
            bw.close();

        }
        catch (Exception ex)
        {
            System.out.println(ex.getMessage());
        }
    }

}




















class Payloadpiece
{
    public int hasPiece;
    public String senderpId;
    public byte[] piece;
    public int pindx;
    public int  getHasPiece()
    {
        return hasPiece;
    }

    public void setHasPiece(int piece) {
        this.hasPiece = piece;
    }
    public String getSenderpId() {
        return senderpId;
    }

    public void setSenderpId(String senderpId) {
        this.senderpId = senderpId;
    }
    public static Payloadpiece convertToPiece(byte[] data)
    {
        int a=Constants.maxPieceLength;
        Payloadpiece p=new Payloadpiece();
        byte[] b=new byte[a];
        System.arraycopy(data,0,b,0,a);
        p.pindx=Constants.convertByteArrayToInt(b,0);
        p.piece=new byte[data.length-a];
        System.arraycopy(data,a,p.piece,0,data.length-a);
        return p;
    }

    public Payloadpiece()
    {
        piece=new byte[Constants.pieceSize];
        pindx=-1;
        hasPiece=0;
        senderpId=null;
    }


}
