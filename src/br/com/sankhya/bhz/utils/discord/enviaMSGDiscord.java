package br.com.sankhya.bhz.utils.discord;

import okhttp3.*;

import java.io.IOException;

public class enviaMSGDiscord {
    public static void Enviar(String mensagem, String token) throws IOException {

        if(null==token) {
            token = "https://discordapp.com/api/webhooks/1327579579673477142/8zyZ2Wi4xALSM4Hccw6LMWdpWss3ki7sx1BL_K_N7YGIgo1F_MYKFkO3Xq9_WJxOs8Hk";
        }

        OkHttpClient client = new OkHttpClient().newBuilder()
                .build();
        MediaType mediaType = MediaType.parse("application/json");
        RequestBody body = RequestBody.create(mediaType, "{\r\n  \"content\": \""+mensagem.replaceAll("\"", "`") +"\"\r\n}");
        Request request = new Request.Builder()
                .url(token)
                .method("POST", body)
                .addHeader("Content-Type", "application/json")
                .build();
        Response response = client.newCall(request).execute();

    }
}
