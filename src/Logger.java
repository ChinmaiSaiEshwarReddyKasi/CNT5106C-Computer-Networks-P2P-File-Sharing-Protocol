import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Calendar;

public class Logger
{
    static FileOutputStream file;
    static OutputStreamWriter writer;
    Logger(String fileName) throws Exception
    {
        File dir = new File("logs");
        dir.mkdir();
        File logsFile = new File("logs", fileName);
        file=new FileOutputStream("logs//"+fileName);
        writer=new OutputStreamWriter(file, StandardCharsets.UTF_8);
    }
    public void printLog(String s)
    {
        try
        {
            writer.write(s+"\n");
        }
        catch (Exception ex)
        {
            ex.printStackTrace();
            System.out.println(ex.getMessage());
        }
    }
    public  void closeLog()
    {
        try
        {
            writer.flush();
            file.close();
        }
        catch (Exception ex)
        {
            ex.printStackTrace();
            System.out.println(ex.getMessage());
        }
    }
    public  void showLog(String message)
    {
        Calendar c=Calendar.getInstance();
        SimpleDateFormat d=new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
        printLog(d.format(c.getTime())+" Peer "+ message);
        System.out.println(d.format(c.getTime())+" Peer "+ message);
    }
}
