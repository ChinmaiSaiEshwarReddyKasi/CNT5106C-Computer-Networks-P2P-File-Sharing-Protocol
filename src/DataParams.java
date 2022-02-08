public class DataParams {
    public MessageData getM() {
        return m;
    }

    public void setM(MessageData m) {
        this.m = m;
    }

    public String getpId() {
        return pId;
    }

    public void setpId(String pId) {
        this.pId = pId;
    }

    MessageData m;
    String pId;

    DataParams() {
        m = new MessageData();
        pId = null;
    }

}