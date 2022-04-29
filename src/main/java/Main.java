import com.alibaba.fastjson.JSON;
import com.google.gson.Gson;
import org.elasticsearch.client.RestClient;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DeliverCallback;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPInputStream;


class Info
{
    String link = null;
    String Content = null;
    String header = null;
}
class NewsContent
{
    String date;
    String Source;
    String Content;

}


class Connector {
    String Page;

    String ParseGzipData(byte[] Data) throws IOException {
        String responsebody = null;
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        int len;
        byte[] buffer = new byte[1024];
        ByteArrayInputStream bin = new ByteArrayInputStream(Data);
        GZIPInputStream gzipper = new GZIPInputStream(bin);
        System.out.println("successfully read");

        while ((len = gzipper.read(buffer)) > 0 )
        {
            out.write(buffer, 0, len);
        }

        gzipper.close();
        out.close();
        responsebody = out.toString();
        return responsebody;
    }
    Connector(String url) throws IOException, InterruptedException {

        HttpClient client = HttpClient.newHttpClient();

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .build();
        HttpResponse<String> response =
                client.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() == 200)
        {
            if (response.headers().toString().contains("gzip"))
            {
                Page  = ParseGzipData(response.toString().getBytes());
            }

            Page = response.body();

        }
    }
    String GetPage(String url) throws IOException, InterruptedException {
        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .build();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        return  response.body().toString();
    }

}
class NewsParser{
    String GetContent(String html)
    {
        String tmp = "";
        Document doc = Jsoup.parse(html);
        Elements news = doc.select("div.article__text>" +
                "div.article__item>p");
        for (Element i : news)
        {
            tmp +=i.html();
        }
        tmp+="\n";
        return  tmp;
    }

    String GetDate(String html)
    {
        Document doc = Jsoup.parse(html);
        Elements Sources = doc.select("span[datetime]");
        return Sources.first().attr("datetime").toString();
    }

    String GetSource(String html)
    {
        Document doc = Jsoup.parse(html);
        Elements Sources = doc.select("span.note>a.link");
        return Sources.first().text().toString();
    }
}
class ContentPrinter extends Thread
{
    public void run()
    {
        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost("localhost");
            factory.setUsername("rabbitmq");
            factory.setPassword("rabbitmq");
            factory.setPort(5672);
            Connection connection = factory.newConnection();
            Channel channel = connection.createChannel();

            channel.queueDeclare("CONTENT_TEST", false, false, true, null);
            System.out.println(" Content Dispatcher is on");
            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                String content = new String(delivery.getBody(), "UTF-8");
                try {
                    System.out.println(content);
                } finally {
                    channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                }
            };
            boolean autoAck = false;
            channel.basicConsume("CONTENT_TEST", autoAck, deliverCallback, consumerTag -> { });
        }
        catch (Exception e)
        {
            return;
        }

    }
}

class ParsePageThread extends Thread
{
    public void AppendContent(String content)
    {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setUsername("rabbitmq");
        factory.setPassword("rabbitmq");
        factory.setPort(5672);
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {
            channel.queueDeclare("CONTENT_TEST", false, false, true, null);
            channel.basicPublish("", "CONTENT_TEST", null, content.getBytes(StandardCharsets.UTF_8));

        }
        catch (Exception e)
        {
            System.out.println("error appending Page");
            return;
        }
    }
    public void run()
    {
        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost("localhost");
            factory.setUsername("rabbitmq");
            factory.setPassword("rabbitmq");
            factory.setPort(5672);
            Connection connection = factory.newConnection();
            Channel channel = connection.createChannel();

            channel.queueDeclare("PAGES_TEST", false, false, true, null);
            System.out.println(" PAGES Dispatcher is on");
            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                String page = new String(delivery.getBody(), "UTF-8");
                try {
                    NewsParser  myparser = new NewsParser();
                    Gson gson = new Gson();
                    NewsContent content= new NewsContent();
                    content.Content = myparser.GetContent(page);
                    content.date = myparser.GetDate(page);
                    content.Source = myparser.GetSource(page);

                    AppendContent(gson.toJson(content).toString());
                } finally {

                    channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                }
            };
            boolean autoAck = false;
            channel.basicConsume("PAGES_TEST", autoAck, deliverCallback, consumerTag -> { });
        }
        catch (Exception e)
        {
            return;
        }
    }
}
class DownloadPageThread extends Thread
{
    public  void AppendPage(String page)
    {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setUsername("rabbitmq");
        factory.setPassword("rabbitmq");
        factory.setPort(5672);
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {
            channel.queueDeclare("PAGES_TEST", false, false, true, null);
            channel.basicPublish("", "PAGES_TEST", null, page.getBytes(StandardCharsets.UTF_8));

        }
        catch (Exception e)
        {
            System.out.println("error appending Page");
            return;
        }


    }
    static String GetPage(String url) throws IOException, InterruptedException {
        Connector pageconnector = new Connector(url);
        return pageconnector.Page;
    }

    public void run()
    {
        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost("localhost");
            factory.setUsername("rabbitmq");
            factory.setPassword("rabbitmq");
            factory.setPort(5672);
            Connection connection = factory.newConnection();
            Channel channel = connection.createChannel();

            channel.queueDeclare("LINKS_TEST", false, false, true, null);
            System.out.println(" LINKS Dispatcher is on !");
            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                String message = new String(delivery.getBody(), "UTF-8");
                try {
                    AppendPage(GetPage(message));
                }
                catch (Exception e)
                {
                    System.out.println(" error downloading page "+ e.toString());
                }
                finally {
                    //System.out.println(" [x] Done");
                    channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                }

            };
            boolean autoAck = false;
            channel.basicConsume("LINKS_TEST", autoAck, deliverCallback, consumerTag -> { });
        }
        catch (Exception e)
        {
            return;
        }
    }
}

public class Main
{
    static String GetPage(String url) throws IOException, InterruptedException {
        Connector pageconnector = new Connector(url);
        return pageconnector.Page;
    }
    static void SendLinkToQueue(String link)
    {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setUsername("rabbitmq");
        factory.setPassword("rabbitmq");
        factory.setPort(5672);
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {
            channel.queueDeclare("LINKS_TEST", false, false, true, null);
            channel.basicPublish("", "LINKS_TEST", null, link.getBytes(StandardCharsets.UTF_8));
        }
        catch (Exception e)
        {
            return;
        }

    }
    static void parser (String html) throws IOException, InterruptedException
    {
        Document doc = Jsoup.parse(html);
        Elements links = doc.select("a.newsitem__title");
        for (Element i : links)
        {
            SendLinkToQueue(i.attr("href").toString());
        }
        links = doc.select("li.list__item>span.list__text>a");
        for (Element i : links)
        {
           SendLinkToQueue(i.attr("href").toString());
        }
    }



    public static void main(String [] args) throws IOException, InterruptedException {

        try
        {

            DownloadPageThread t1 = new DownloadPageThread();
            DownloadPageThread t3 = new DownloadPageThread();
            ParsePageThread t2 = new ParsePageThread();

            ParsePageThread t4 = new ParsePageThread();
            //ContentPrinter t5 = new ContentPrinter();
            t1.run();
            t3.run();
            t2.run();
            t4.run();
           // t5.run();
            while(true)
            {
                parser(GetPage("https://news.mail.ru/"));
                Thread.sleep(10000);
                break;
            }

        }
        catch (Exception e )
        {
            System.out.println("error: "+ e.toString());
        }

    }


}