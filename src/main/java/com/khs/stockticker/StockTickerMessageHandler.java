package com.khs.stockticker;

import com.eclipsesource.json.JsonArray;
import com.eclipsesource.json.JsonObject;
import com.eclipsesource.json.JsonValue;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.gson.Gson;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import org.apache.http.Header;
import org.apache.http.HeaderElement;
import org.apache.http.HttpEntity;
import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.HttpResponse;
import org.apache.http.HttpResponseInterceptor;
import org.apache.http.client.entity.GzipDecompressingEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by jwb on 3/13/15.
 */
public class StockTickerMessageHandler implements WebSocketMessageHandler {
    private static final Logger logger = LoggerFactory.getLogger(StockTickerServerHandler.class);

    // Keep track of the tickers the user has asked for info about
    private List<String> tickerSymbols = new CopyOnWriteArrayList<>();

    // stateless JSON serializer/deserializer
    private Gson gson = new Gson();

    private static final AtomicBoolean keepRunning = new AtomicBoolean(true);

    // Keep track of the current channel so we can talk directly to the client
    private AtomicReference<Channel> channel = new AtomicReference();

    // need an executor for the thread that will intermittently send data to the client
    private ExecutorService executor = Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder()
                    .setDaemon(true)
                    .setNameFormat("ticker-processor-%d")
                    .build()
    );

    public StockTickerMessageHandler() {
        SendResultsCallable toClientCallable = new SendResultsCallable();
        FutureTask<String> toClientPc = new FutureTask<>(toClientCallable);
        executor.execute(toClientPc);
    }

    public String handleMessage(ChannelHandlerContext ctx, String frameText) {
        this.channel.set(ctx.channel());
        TickerResponse tickerResponse = new TickerResponse();
        TickerRequest tickerRequest = gson.fromJson(frameText, TickerRequest.class);

        if (tickerRequest.getCommand() != null) {
            if ("add".equals(tickerRequest.getCommand())) {
                tickerSymbols.add(tickerRequest.getTickerSymbol());
                tickerResponse.setResult("success");
            } else if ("remove".equals(tickerRequest.getCommand())) {
                tickerSymbols.remove(tickerRequest.getTickerSymbol());
                tickerResponse.setResult("success");
            } else {
                tickerResponse.setResult("Failed. Command not recognized.");
            }
        } else {
            tickerResponse.setResult("Failed. Command not recognized.");
        }

        String response = gson.toJson(tickerResponse);
        return response;
    }

    /**
     * This class runs as a thread, and waits for messages to arrive in its queue that are to be sent back to the client.
     *
     * @author jwb
     */
    class SendResultsCallable implements Callable<String> {
        // one per callable as it is stateless, but not thread safe
        private Gson gson = new Gson();

        public SendResultsCallable() {
        }

        @Override
        public String call() throws Exception {
            // keep going until all messages are sent
            while (keepRunning.get()) {
                if (tickerSymbols.size() > 0) {
                    TickerResponse tickerResponse = new TickerResponse();
                    tickerResponse.setResult("success");
                    tickerResponse.setTickerData(getPricesForSymbols(tickerSymbols));

                    String response = gson.toJson(tickerResponse);

                    // send the client an update
                    channel.get().writeAndFlush(new TextWebSocketFrame(response));
                }

                // only try to send back to client every 2 seconds so it isn't overwhelmed with messages
                Thread.sleep(2000L);
            }

            return "done";
        }
    }

    private Map<String, String> getPricesForSymbols(List<String> symbols) {
        Map<String, String> response = new HashMap<>();
        String url = "http://finance.google.com/finance/info?client=ig&q=NASDAQ%3A";
        url += String.join(",", symbols);
        logger.info(url);

        CloseableHttpClient httpClient = HttpClients.custom()
                .addInterceptorFirst((HttpRequestInterceptor) (request, context) -> {
                    if (!request.containsHeader("Accept-Encoding")) {
                        request.addHeader("Accept-Encoding", "gzip");
                    }

                }).addInterceptorFirst(new HttpResponseInterceptor() {

                    public void process(
                            final HttpResponse response,
                            final HttpContext context) throws HttpException, IOException {
                        HttpEntity entity = response.getEntity();
                        if (entity != null) {
                            Header ceheader = entity.getContentEncoding();
                            if (ceheader != null) {
                                HeaderElement[] codecs = ceheader.getElements();
                                for (int i = 0; i < codecs.length; i++) {
                                    if (codecs[i].getName().equalsIgnoreCase("gzip")) {
                                        response.setEntity(
                                                new GzipDecompressingEntity(response.getEntity()));
                                        return;
                                    }
                                }
                            }
                        }
                    }
                }).build();

        try {
            HttpUriRequest query = RequestBuilder.get()
                    .setUri(url)
                    .build();
            CloseableHttpResponse queryResponse = httpClient.execute(query);
            try {
                HttpEntity entity = queryResponse.getEntity();
                if (entity != null) {
                    String data = EntityUtils.toString(entity);
                    // google finance api response starts with a blank comment
                    data = data.replace("//", "");
                    JsonArray quotes = JsonArray.readFrom(data);
                    if (quotes.isArray()) {
                        logger.info("JSON ARRAY");
                    }
                    for (JsonValue quote: quotes) {
                        String symbol = quote.asObject().get("t").asString();
                        String last = quote.asObject().get("l").asString();
                        response.put(symbol, last);
                    }
                }
            } finally {
                queryResponse.close();
            }
        } catch (Exception e) {
            logger.info(e.getMessage());
        } finally {
            try {
                httpClient.close();
            } catch (Exception e) {
            }
        }

        return response;
    }
}
