import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;

public class P2P
{
    public static HashMap<String,RemotePeerInfo> remotePeerInfoHashMap=new HashMap<>();
    public  static HashMap<String,RemotePeerInfo> preferredNeighboursInfoHashMap=new HashMap<>();
    static  Logger l;
    public static volatile Timer timer;
    public static PayLoadData currentDataPayLoad=null;
    static int clientPort;
    public static ServerSocket socket=null;
    public static Thread thread;
    public static String peerId;
    public static boolean finishedFlag = false;
    public static Queue<DataParams> queue = new LinkedList<>();
    public static Vector<Thread> st=new Vector<>();
    public static Thread mp;
    public  static Vector<Thread> pt=new Vector<>();
    public static HashMap<String,Socket> peerData=new HashMap<>();
    public static volatile Hashtable<String, RemotePeerInfo> preferredNeighboursHashMapTable = new Hashtable<>();
    public static volatile Hashtable<String, RemotePeerInfo> unchokedNeighboursHashMapTable = new Hashtable<>();
    public static void main(String args[]) throws Exception
    {
         peerId=args[0];
       l =new Logger("Peer_"+peerId+".log");
       boolean flag=false;
        try
        {

            l.showLog(peerId+" is started");
            getConfigData();
            getPeerInfoDate();            
            setPreferredNeighbours(peerId);
            x:for(Map.Entry<String,RemotePeerInfo> hm: remotePeerInfoHashMap.entrySet() )
            {
                RemotePeerInfo r = hm.getValue();
                if (r.peerId.equals(peerId))
                {
                    clientPort = Integer.parseInt(r.peerPort);


                    if (r.hasFile)
                    {
                      flag=true;
                      break x;
                    }
                }
            }
            currentDataPayLoad=new PayLoadData();
            currentDataPayLoad.initPayLoad(peerId,flag);
            Thread t=new Thread(new DataController(peerId));
            t.start();
            if(flag)
            {
                try
                {
                    P2P.socket = new ServerSocket(clientPort);
                    thread = new Thread(new Server(P2P.socket, peerId));
                    thread.start();
                }
                catch (Exception ex)
                {
                   l.showLog(peerId+ " peer is getting an exception while starting the thread");
                    l.closeLog();
                    System.exit(0);
                }
            }
            else
            {
                generatePeerFile();
                for(Map.Entry<String,RemotePeerInfo> hm: remotePeerInfoHashMap.entrySet() )
                {
                    RemotePeerInfo remotePeerInfo=hm.getValue();
                    if(Integer.parseInt(peerId)>Integer.parseInt(hm.getKey()))
                    {
                        PeerController p=new PeerController(remotePeerInfo.getPeerAddress(),Integer.parseInt(remotePeerInfo.getPeerPort()),1, peerId);
                        Thread temp=new Thread(p);
                        pt.add(temp);
                        temp.start();
                    }

                }
                try
                {
                    P2P.socket = new ServerSocket(clientPort);
                    thread = new Thread(new Server(P2P.socket, peerId));
                    thread.start();
                }
                catch (Exception ex)
                {
                    l.showLog(peerId+ " peer is getting an exception while starting the thread");
                    l.closeLog();
                    System.exit(0);
                }
                

            }
            setPreferredNeighbors();
            setUnChokedNeighbors();
              Thread cThread=thread;
            Thread mp=t;
            while(true) {
                finishedFlag = isFinished();
                if (finishedFlag) {
                    l.showLog("All peers have completed downloading the file.");

                    terminatePreferredNeighbors();
                    terminateUnchokedNeighbors();

                    try {
                        Thread.currentThread();
                        Thread.sleep(2000);
                    } catch (InterruptedException ignored) {
                    }

                    if (cThread.isAlive())
                       cThread.stop();

                    if (mp.isAlive())
                        mp.stop();

                    for (Thread thread : pt)
                        if (thread.isAlive())
                            thread.stop();

                    for (Thread thread : st)
                        if (thread.isAlive())
                            thread.stop();

                    break;
                } else {
                    try {
                        Thread.currentThread();
                        Thread.sleep(5000);
                    } catch (InterruptedException ignored) {}
                }
            }
        }
        catch(Exception exception) {
            l.showLog(String.format(peerId+" is ending with error : "+ exception.getMessage()));
        }
        finally {
            l.showLog(String.format(peerId+" Peer is terimating."));
            l.closeLog();
            System.exit(0);
        }
    }

    

    

    private static void getPeerInfoDate() throws IOException {
        String configs;
        BufferedReader b = null;

        try
        {
            b=new BufferedReader(new FileReader(Constants.PEERS_PATH));
            while((configs=b.readLine())!=null)
            {
                String[] line=configs.split(" ");
                remotePeerInfoHashMap.put(line[0],new RemotePeerInfo(line[0],line[1],line[2],line[3].equals("1")));

            }
        }
        catch (Exception ex)
        {
            l.showLog(ex.getMessage());
        }
        finally
        {
            b.close();
        }
    }
    private static void generatePeerFile()
    {
        try
        {
            File f=new File(peerId,Constants.fileName);
            OutputStream fop=new FileOutputStream(f,true);
            byte intialByte=0;
            int i=0;
            while(i<Constants.fileSize)
            {
                fop.write(intialByte);
                i++;
            }
            fop.close();

        }
        catch (Exception e)
        {
            l.showLog("Error while creating intial dummy file for peer "+peerId);
        }
    }
    private static void setPreferredNeighbours(String pId)
    {
        for(Map.Entry<String,RemotePeerInfo> hm: remotePeerInfoHashMap.entrySet() )
        {
            if (!hm.getKey().equals(pId)) {
                preferredNeighboursInfoHashMap.put(hm.getKey(), hm.getValue());
            }
        }
    }
    public static void getConfigData() throws IOException {
        String configs;
        BufferedReader b = null;
        try
        {
             b=new BufferedReader(new FileReader(Constants.COMMON_CONFIG_PATH));
            while((configs=b.readLine())!=null)
            {

                String[] line=configs.split(" ");
                if(line[0].trim().equals("NumberOfPreferredNeighbors"))
                {
                    Constants.numberOfPreferredNeighbors=Integer.parseInt(line[1]);
                }
                if(line[0].trim().equals("UnchokingInterval"))
                {
                    Constants.unchokingInterval=Integer.parseInt(line[1]);
                }
                if(line[0].trim().equals("OptimisticUnchokingInterval"))
                {
                    Constants.optimisticUnchokingInterval=Integer.parseInt(line[1]);
                }
                if(line[0].trim().equals("FileName"))
                {
                    Constants.fileName=line[1];
                }
                if(line[0].trim().equals("FileSize"))
                {
                    Constants.fileSize=Integer.parseInt(line[1]);
                }
                if(line[0].trim().equals("PieceSize"))
                {
                    Constants.pieceSize=Integer.parseInt(line[1]);
                }
            }
        }
        catch (Exception ex)
        {
           l.showLog(ex.getMessage());
        }
        finally
        {
            b.close();
        }
    }
    public static synchronized DataParams removeDataFromQueue(){
        DataParams dp = null;
        if(queue.isEmpty()){}
        else {
            dp = queue.remove();
        }
        return dp;
    }
    public static synchronized void addToQueue(DataParams dp)
    {
        queue.add(dp);
    }
    
    public static class SetPreferredNeighbours extends TimerTask {
        public void run() {
            readNextPeerData();
            Enumeration<String> remotepeerIds = Collections.enumeration(remotePeerInfoHashMap.keySet());
            int pinterested = 0;
            StringBuilder s = new StringBuilder();
            while(remotepeerIds.hasMoreElements()) {
                String remotePeerId = remotepeerIds.nextElement();
                RemotePeerInfo remotepeer = remotePeerInfoHashMap.get(remotePeerId);
                if(remotePeerId.equals(peerId)) continue;
                if (remotepeer.isCompleted == 0 && remotepeer.isHandShake == 1)
                    pinterested++;
                else if(remotepeer.isCompleted == 1) {
                    try {
                        preferredNeighboursInfoHashMap.remove(remotePeerId);
                    }
                    catch (Exception ignored) { }
                }
            }
            if(pinterested > Constants.piece) 
            {
                
                if(!preferredNeighboursInfoHashMap.isEmpty())
                    preferredNeighboursInfoHashMap.clear();

                List<RemotePeerInfo> remotePeersarrayList = new ArrayList<>(remotePeerInfoHashMap.values());
                remotePeersarrayList.sort(new RemotePeerInfo());
                int Count = 0;

                for (RemotePeerInfo remotePeerInfo : remotePeersarrayList) 
                {
                    if (Count > Constants.numberOfPreferredNeighbors - 1) break;

                    if (remotePeerInfo.isHandShake == 1 && !remotePeerInfo.peerId.equals(peerId)
                            && remotePeerInfoHashMap.get(remotePeerInfo.peerId).isCompleted == 0) 
                            {
                        remotePeerInfoHashMap.get(remotePeerInfo.peerId).isPreferredNeighbor = 1;
                        preferredNeighboursInfoHashMap.put(remotePeerInfo.peerId, remotePeerInfoHashMap.get(remotePeerInfo.peerId));
                        Count++;
                        s.append(remotePeerInfo.peerId).append(", ");

                        if (remotePeerInfoHashMap.get(remotePeerInfo.peerId).isChoked == 1) 
                        {
                            sendRequestToUnchoke(remotePeerInfo.peerId,P2P.peerData.get(remotePeerInfo.peerId) );
                            P2P.remotePeerInfoHashMap.get(remotePeerInfo.peerId).isChoked = 0;
                            sendHaveMessage(remotePeerInfo.peerId,P2P.peerData.get(remotePeerInfo.peerId));
                            P2P.remotePeerInfoHashMap.get(remotePeerInfo.peerId).state = 3;
                        }
                    }
                }
            }
            else
            {
                remotepeerIds = Collections.enumeration(remotePeerInfoHashMap.keySet());
                while(remotepeerIds.hasMoreElements())
                {
                    String nextPeerId = remotepeerIds.nextElement();
                    RemotePeerInfo remotePeer = remotePeerInfoHashMap.get(nextPeerId);
                    if(nextPeerId.equals(peerId)) continue;

                    if (remotePeer.isCompleted == 0 && remotePeer.isHandShake == 1) {
                        if(!preferredNeighboursInfoHashMap.containsKey(nextPeerId)) {
                            s.append(nextPeerId).append(", ");
                            preferredNeighboursInfoHashMap.put(nextPeerId, remotePeerInfoHashMap.get(nextPeerId));
                            remotePeerInfoHashMap.get(nextPeerId).isPreferredNeighbor = 1;
                        }
                        if (remotePeer.isChoked == 1) {
                            sendRequestToUnchoke(nextPeerId,P2P.peerData.get(nextPeerId));
                            P2P.remotePeerInfoHashMap.get(nextPeerId).isChoked = 0;
                            sendHaveMessage(nextPeerId,P2P.peerData.get(nextPeerId));
                            P2P.remotePeerInfoHashMap.get(nextPeerId).state = 3;
                        }
                    }
                }
            }
            if (!s.toString().equals(""))
                l.showLog( P2P.peerId+" has selected the preferred neighbors "+ s);
        }
    }

    private static void sendRequestToUnchoke( String remotePeerID,Socket socket) {
        l.showLog(peerId+" is sending UNCHOKE message to Peer "+ remotePeerID);
        
        sendOutput( MessageData.convertDataToByteArray(new MessageData(Constants.unChoke)),socket);
    }

    private static void sendHaveMessage( String remotePeerID,Socket socket) {
        byte[] b = P2P.currentDataPayLoad.encodeData();
        l.showLog(peerId+" is sending HAVE message to Peer "+ remotePeerID);
        
        sendOutput( MessageData.convertDataToByteArray(new MessageData(Constants.have, b)),socket);
    }

    
    public static void readNextPeerData() {
        try {
            String peerDetail;
            BufferedReader br = new BufferedReader(new FileReader(Constants.PEERS_PATH));
            while ((peerDetail = br.readLine()) != null) {
                String[]p = peerDetail.trim().split(" ");
                String peerID = p[0];
                if(Integer.parseInt(p[3]) == 1) {
                    remotePeerInfoHashMap.get(peerID).isCompleted = 1;
                    remotePeerInfoHashMap.get(peerID).isInterested = 0;
                    remotePeerInfoHashMap.get(peerID).isChoked = 0;
                }
            }
            br.close();
        }
        catch (Exception exception) {
            l.showLog(peerId + "" +exception.toString());
        }
    }
    public static synchronized boolean isFinished() {
        String peerDetail;
        int Count = 1;

        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(
                    Constants.PEERS_PATH));
            while ((peerDetail = bufferedReader.readLine()) != null) {
                Count = Count
                        * Integer.parseInt(peerDetail.trim().split(" ")[3]);
            }
            bufferedReader.close();
            return Count != 0;
        } catch (Exception e) {
            l.showLog(e.toString());
            return false;
        }
    }
    public static class setUNChokedNeighbors extends TimerTask {

        public void run() {
            readNextPeerData();
            if(!unchokedNeighboursHashMapTable.isEmpty())
                unchokedNeighboursHashMapTable.clear();
            Enumeration<String> remotePeerIds = Collections.enumeration(remotePeerInfoHashMap.keySet());
            Vector<RemotePeerInfo> remotePeerVector = new Vector<>();
            while(remotePeerIds.hasMoreElements()) {
                String key = remotePeerIds.nextElement();
                RemotePeerInfo remotePeerInfo = remotePeerInfoHashMap.get(key);
                if (remotePeerInfo.isChoked == 1
                        && !key.equals(peerId)
                        && remotePeerInfo.isCompleted == 0
                        && remotePeerInfo.isHandShake == 1)
                    remotePeerVector.add(remotePeerInfo);
            }

            if (remotePeerVector.size() > 0) {
                Collections.shuffle(remotePeerVector);
                RemotePeerInfo intialPeer = remotePeerVector.firstElement();
                remotePeerInfoHashMap.get(intialPeer.peerId).isOptUnchokedNeighbor = 1;
                unchokedNeighboursHashMapTable.put(intialPeer.peerId, remotePeerInfoHashMap.get(intialPeer.peerId));
                P2P.l.showLog( P2P.peerId+" has the optimistically unchoked neighbor "+intialPeer.peerId);

                if (remotePeerInfoHashMap.get(intialPeer.peerId).isChoked == 1) {
                    P2P.remotePeerInfoHashMap.get(intialPeer.peerId).isChoked = 0;
                    sendRequestToUnchoke( intialPeer.peerId,P2P.peerData.get(intialPeer.peerId));
                    sendHaveMessage(intialPeer.peerId,P2P.peerData.get(intialPeer.peerId));
                    P2P.remotePeerInfoHashMap.get(intialPeer.peerId).state = 3;
                }
            }
        }
    }

    public static void setUnChokedNeighbors() {
        timer = new Timer();
        timer.schedule(new setUNChokedNeighbors(),
                0,Constants.optimisticUnchokingInterval * 1000L);
    }

    public static void terminateUnchokedNeighbors() {
        timer.cancel();
    }

    public static void setPreferredNeighbors() {
        timer = new Timer();
        timer.schedule(new SetPreferredNeighbours(),
                0,Constants.unchokingInterval * 1000L);
    }

    public static void terminatePreferredNeighbors() {
        timer.cancel();
    }
  
    private static void sendOutput(byte[] b,Socket socket) {
        try {
            OutputStream outputStream = socket.getOutputStream();
            outputStream.write(b);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }
    
  

   

}
