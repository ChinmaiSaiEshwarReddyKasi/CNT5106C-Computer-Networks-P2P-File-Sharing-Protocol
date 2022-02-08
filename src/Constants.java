public class Constants {
    public static final String PEERS_PATH = "PeerInfo.cfg";
    public static final String COMMON_CONFIG_PATH = "CommonConfig.cfg";
    public static final String zeroBits = "0000000000";
    public static final String handshakeHeader = "P2PFILESHARINGPROJ";
    public  static int numberOfPreferredNeighbors = 0;
    public  static int unchokingInterval=0;
    public  static int optimisticUnchokingInterval=0;
    public  static int fileSize=0;
    public  static int pieceSize=0;
    public  static String fileName="";
    public static final int maxPieceLength=4;
    public static final int sizeoOfHandShakeMessage=32;
    public static final int sizeOfHeader=18;
    public static final int sizeofZerobits=10;
    public static final int sizeOfPeerId=4;
    public static final int sizeOfMessage=4;
    public static final int typeOfMessage=1;
    public static final int choke=0;
    public static final int unChoke=1;
    public static final int intersted=2;
    public static final int notInterested=3;
    public static final int have=4;
    public static final int bitField=5;
    public static final int request=6;
    public static final int piece=7;
    public static int convertByteArrayToInt(byte[] data, int offset)
    {
        int res=0;
        for(int j=0;j<4;j++)
        {
            int s=(3-j)*8;
            res+=(data[j + offset] & 0x000000FF) << s;
        }
        return res;
    }
    public static byte[] convertIntToByte(int val)
    {
       byte[] b=new byte[4];
       int i=0;
       while(i<4)
       {
           int o=(b.length-1-i)*8;
           b[i]=(byte)((val>>>o)&(0xFF));
           i++;
       }
       return b;
    }
}
