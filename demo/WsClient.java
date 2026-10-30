import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;

/**
 * Minimal reference client for the demo: connects, sends RESUME for one topic and prints every frame.
 *
 *   java demo/WsClient.java <ws-url> <jwt> <topic> [lastSeq=0]
 *
 * Remember the highest STATUS/SOS "seq" you saw and pass it as lastSeq on the next run: that is the
 * whole resume protocol. (LOCATION frames are latest-only and are not used as a resume point.)
 */
public class WsClient {
    public static void main(String[] args) throws Exception {
        String url = args[0], token = args[1], topic = args[2];
        long lastSeq = args.length > 3 ? Long.parseLong(args[3]) : 0;
        CountDownLatch closed = new CountDownLatch(1);
        HttpClient.newHttpClient().newWebSocketBuilder()
                .buildAsync(URI.create(url + "/ws/updates?token=" + token), new WebSocket.Listener() {
                    StringBuilder buf = new StringBuilder();

                    @Override
                    public void onOpen(WebSocket ws) {
                        System.out.println("connected; resuming " + topic + " from seq " + lastSeq);
                        ws.sendText("{\"action\":\"RESUME\",\"topic\":\"" + topic + "\",\"lastSeq\":" + lastSeq + "}", true);
                        ws.request(1);
                    }

                    @Override
                    public CompletionStage<?> onText(WebSocket ws, CharSequence data, boolean last) {
                        buf.append(data);
                        if (last) {
                            System.out.println(java.time.LocalTime.now().withNano(0) + "  " + buf);
                            buf.setLength(0);
                        }
                        ws.request(1);
                        return null;
                    }

                    @Override
                    public CompletionStage<?> onClose(WebSocket ws, int code, String reason) {
                        System.out.println("closed: " + code + " " + reason);
                        closed.countDown();
                        return null;
                    }

                    @Override
                    public void onError(WebSocket ws, Throwable t) {
                        System.out.println("error: " + t);
                        closed.countDown();
                    }
                }).join();
        closed.await();
    }
}
