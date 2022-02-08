import java.nio.charset.StandardCharsets;

public class MessageData {


    private byte[] dataArray;
    private byte[] dataLengthArray;
    private byte[] payLoadArray;
    private String dataLength;
    private String dataType;
    private int lengthOfMessage = Constants.typeOfMessage;

    MessageData() {

    }

    MessageData(int n) {
        try {
            if ((n == Constants.choke) || (n == Constants.unChoke) || (n == Constants.intersted) || (n == Constants.notInterested)) {
                this.setDataType("" + n);
                this.payLoadArray = null;
                this.lengthOfMessage = 1;
                this.dataLength = this.lengthOfMessage + "";
                this.dataLengthArray = Constants.convertIntToByte(this.lengthOfMessage);
            } else {
                System.out.println("Invalid Message");
            }
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        }
    }

    MessageData(int n, byte[] arr) {
        try {
            if (arr == null) {
                if ((n == Constants.choke) || (n == Constants.unChoke) || (n == Constants.intersted) || (n == Constants.notInterested)) {
                    this.lengthOfMessage = 1;
                    this.dataLength = this.lengthOfMessage + "";
                    this.dataLengthArray = Constants.convertIntToByte(this.lengthOfMessage);
                    this.payLoadArray = null;
                } else {
                    System.out.println("Empty PayLoad");
                }

            } else {
                this.lengthOfMessage = arr.length + 1;
                this.dataLength = this.lengthOfMessage + "";
                this.dataLengthArray = Constants.convertIntToByte(this.lengthOfMessage);
                if (this.dataLengthArray.length > Constants.sizeOfMessage) {
                    System.out.println("Message length greater than size required");
                }
                this.payLoadArray = arr;
            }
            this.setDataType(""+n);

        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        }
    }

    public int getLengthOfMessage() {
        return lengthOfMessage;
    }

    public void setLengthOfMessage(int lengthOfMessage) {
        this.lengthOfMessage = lengthOfMessage;
    }

    public byte[] getDataLengthArray() {
        return dataLengthArray;
    }

    public void setDataLengthArray(byte[] dataLengthArray) {
        this.dataLengthArray = dataLengthArray;
    }

    public byte[] getDataArray() {
        return dataArray;
    }

    public void setDataArray(byte[] dataArray) {
        this.dataArray = dataArray;
    }

    public String getDataType() {
        return dataType;
    }

    public void setDataType(byte[] data) {
        this.dataType = new String(data, StandardCharsets.UTF_8);
        this.dataArray = data;
    }

    public void setDataType(String data) {
        this.dataType = data.trim();
        this.dataArray = this.dataType.getBytes(StandardCharsets.UTF_8);
    }

    public String getDataLength() {
        return dataLength;
    }

    public void setDataLength(byte[] b) {
        int l = Constants.convertByteArrayToInt(b, 0);
        this.dataLength = "" + l;
        this.dataLengthArray = b;
        this.lengthOfMessage = l;
    }

    public void setDataLength(String data) {
        this.lengthOfMessage = Integer.parseInt(data);
        this.dataLength = data;
        this.dataLengthArray = Constants.convertIntToByte(this.lengthOfMessage);
    }

    public byte[] getPayLoadArray() {
        return payLoadArray;
    }

    public void setPayLoadArray(byte[] payLoadArray) {
        this.payLoadArray = payLoadArray;
    }

    public static byte[] convertDataToByteArray(MessageData m) {
        byte[] dataByteArray;
        int dType;
        try {
            dType = Integer.parseInt(m.getDataType());
            if ((m.getDataArray() == null) || ((dType < 0) || dType > 7) || (m.getDataLengthArray().length > Constants.sizeOfMessage) || (m.getDataLengthArray() == null)) {
                throw new Exception("Message is Not Valid");
            }
            if (m.getPayLoadArray() == null) {
                dataByteArray = new byte[Constants.sizeOfMessage + Constants.typeOfMessage];
                System.arraycopy(m.getDataLengthArray(), 0, dataByteArray, 0, m.getDataLengthArray().length);
                System.arraycopy(m.getDataArray(), 0, dataByteArray, Constants.sizeOfMessage, Constants.typeOfMessage);
            } else {
                dataByteArray = new byte[Constants.sizeOfMessage + Constants.typeOfMessage + m.getPayLoadArray().length];
                System.arraycopy(m.getDataLengthArray(), 0, dataByteArray, 0, m.getDataLengthArray().length);
                System.arraycopy(m.getDataArray(), 0, dataByteArray, Constants.sizeOfMessage, Constants.typeOfMessage);
                System.arraycopy(m.getPayLoadArray(), 0, dataByteArray, Constants.sizeOfMessage + Constants.typeOfMessage, m.getPayLoadArray().length);
            }
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
            dataByteArray = null;
        }
        return dataByteArray;
    }

    public static MessageData convertByteArrayToData(byte[] b)
    {
        MessageData m=new MessageData();
        byte[] dlen=new byte[Constants.sizeOfMessage];
        byte[] dType=new byte[Constants.typeOfMessage];
        byte[] payload;
        int mlen;
        try
        {
            if(b.length<Constants.sizeOfMessage+Constants.typeOfMessage || b==null)
            {
                throw new Exception("Invalid Message");
            }
            System.arraycopy(b,0,dlen,0,Constants.sizeOfMessage);
            System.arraycopy(b,Constants.sizeOfMessage,dType,0,Constants.typeOfMessage);
            m.setDataLength(dlen);
            m.setDataType(dType);
            mlen=Constants.convertByteArrayToInt(dlen,0);
            if(mlen>1)
            {
                payload=new byte[mlen-1];
                System.arraycopy(b,Constants.sizeOfMessage+Constants.typeOfMessage,payload,0,b.length-Constants.sizeOfMessage-Constants.typeOfMessage);
                m.setPayLoadArray(payload);
            }
        }
        catch (Exception ex) {
            System.out.println(ex.getMessage());

        }
        return m;
    }
}

