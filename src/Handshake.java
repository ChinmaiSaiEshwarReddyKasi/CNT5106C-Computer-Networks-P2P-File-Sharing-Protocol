import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;

public class Handshake extends Constants
{
    public byte[] getHandShakeMessage() {
        return handShakeMessage;
    }

    public void setHandShakeMessage(byte[] handShakeMessage) {
        this.handShakeMessage = handShakeMessage;
    }

    public int getPeerId() {
        return peerId;
    }

    public void setPeerId(int peerId) {
        this.peerId = peerId;
    }

    public String getHandShakeHeader() {
        return handShakeHeader;
    }

    public void setHandShakeHeader(String handShakeHeader) {
        this.handShakeHeader = handShakeHeader;
    }

    public String getZeroBits() {
        return zeroBits;
    }

    public void setZeroBits(String zeroBits) {
        this.zeroBits = zeroBits;
    }

    @Override
    public String toString() {
        return "Handshake{" +
                "handShakeMessage=" + Arrays.toString(handShakeMessage) +
                '}';
    }

    private byte[] handShakeMessage;
    private int peerId;
    private String handShakeHeader;
    private String zeroBits;
    private int k;

    public byte[] getHandShakeHeaderBytes() {
        return handShakeHeaderBytes;
    }

    public void setHandShakeHeaderBytes(byte[] handShakeHeaderBytes) {
        this.handShakeHeaderBytes = handShakeHeaderBytes;
    }

    private byte[] handShakeHeaderBytes=new byte[32];
    Handshake()
    {

    }

    Handshake(int peerId)
    {
        this.handShakeMessage=new byte[32];
        this.peerId=peerId;
        this.handShakeHeader=Constants.handshakeHeader;
        this.zeroBits=Constants.zeroBits;
        this.k=0;
        this.handShakeHeaderBytes=handShakeHeader.getBytes(StandardCharsets.UTF_8);
    }

    public void generateHandShake()
    {
        byte[] handShakeHeaderByteArray=this.handShakeHeader.getBytes();
        byte[] zeroBitsByteArray=this.zeroBits.getBytes(StandardCharsets.UTF_8);
        String peerIdString=this.peerId+"";
        byte[] peerIdByteArray=peerIdString.getBytes(StandardCharsets.UTF_8);
        int k=0;
        try
        {
            setHandShakeMessageHeader(handShakeHeaderByteArray);
        }
        catch (Exception ex)
        {
            ex.printStackTrace();
            System.out.println(ex.getMessage());
        }
        try
        {
            setHandShakeMessagePaddng(zeroBitsByteArray);
        }
        catch (Exception ex)
        {
            ex.printStackTrace();
            System.out.println(ex.getMessage());
        }
        try
        {
            setHandShakeMessagepeerId(peerIdByteArray);
        }
        catch (Exception ex)
        {
            ex.printStackTrace();
            System.out.println(ex.getMessage());
        }
        System.out.println("Hand Shake message generated is : "+ new String(this.handShakeMessage, StandardCharsets.UTF_8));
    }


    public void setHandShakeMessageHeader(byte[] handShakeHeaderByteArray)
    {
        try
        {
            if(handShakeHeaderByteArray==null )
            {
                throw new Exception("Please define valid Hand Shake Header");
            }
            if(handShakeHeaderByteArray.length>18 )
            {
                throw new Exception(" Hand Shake Header length is greater than 18 bytes");
            }

            for (int i = 0; i < handShakeHeaderByteArray.length; i++)
            {
                this.handShakeMessage[k] = handShakeHeaderByteArray[i];
                k++;
            }

        }

        catch (Exception e)
        {
            e.printStackTrace();
            System.out.println(e.getMessage());
        }
    }

    public void setHandShakeMessagePaddng(byte[] zeroBitsByteArray)
    {
        try
        {
            if (zeroBitsByteArray == null)
            {
                throw new Exception("Please define valid Zero bit padding");
            }
            if(zeroBitsByteArray.length>10)
            {
               throw new Exception("Zero bit padding length is greater than 10");
            }
            for (int i = 0; i < zeroBitsByteArray.length; i++)
            {
                this.handShakeMessage[k] = zeroBitsByteArray[i];
                k++;
            }
        }
         catch (Exception e)
        {
        e.printStackTrace();
        System.out.println(e.getMessage());
        }

    }


    public void setHandShakeMessagepeerId(byte[] peerIdByteArray)
    {
        try
        {
            if (peerIdByteArray == null)
            {
                throw new Exception("Please define valid PeerId");
            }
            if (peerIdByteArray.length > 4)
            {
                throw new Exception("Zero bit padding length is greater than 10");
            }

            for (int i = 0; i < peerIdByteArray.length; i++) {
                this.handShakeMessage[k] = peerIdByteArray[i];
                k++;
            }
        }
         catch (Exception e)
        {
        e.printStackTrace();
        System.out.println(e.getMessage());
        }

    }
    public static Handshake byteToHandShake(byte[] b)
    {

        byte[] mheader;
        byte[] mpeerId;
        Handshake h;
        if(b.length!=Constants.sizeoOfHandShakeMessage)
        {
            P2P.l.showLog("INVALID length of HandShake Message");
            System.exit(0);
        }
        h=new Handshake();
        mheader=new byte[Constants.sizeOfHeader];
        mpeerId=new byte[Constants.sizeOfPeerId];
        System.arraycopy(b,0,mheader,0,Constants.sizeOfHeader);
        System.arraycopy(b,Constants.sizeOfHeader+Constants.sizeofZerobits,mpeerId,0,Constants.sizeOfPeerId);
        h.setHandShakeMessageHeader(mheader);
        h.setHandShakeMessagepeerId(mpeerId);
        return h;
    }
    public  static byte[] handShakeToArray(Handshake handshake)
    {
        byte[] m=new byte[Constants.sizeoOfHandShakeMessage];


            if(handshake.getHandShakeHeader().length()>Constants.sizeOfHeader||handshake.getHandShakeHeader()==null||handshake.getHandShakeHeader().length()==0)
            {
                P2P.l.showLog("HandShake header not VALID");
                System.exit(0);
            }
            else
            {
                System.arraycopy(handshake.getHandShakeHeader().getBytes(StandardCharsets.UTF_8),0,m,0,handshake.getHandShakeHeader().length());
            }
        if(handshake.getZeroBits()==null || handshake.getZeroBits().isEmpty()||handshake.getZeroBits().length()>Constants.sizeofZerobits)
        {
            P2P.l.showLog("INVALID Zero bits");
            System.exit(0);
        }
        else
        {
            System.arraycopy(handshake.getZeroBits().getBytes(StandardCharsets.UTF_8),0,m,Constants.sizeOfHeader,Constants.sizeofZerobits);


        }
        if( (String.valueOf(handshake.getPeerId())).length()>Constants.sizeOfPeerId)
        {
            P2P.l.showLog("INVALID Peer bits");
            System.exit(0);
        }
        else
        {
           System.arraycopy(String.valueOf(handshake.getPeerId()).getBytes(StandardCharsets.UTF_8),0,m,Constants.sizeOfHeader+Constants.sizeofZerobits,Constants.sizeOfPeerId);
        }
        return m;
    }

}
