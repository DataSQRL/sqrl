package com.datasqrl.auth;

public class AuthUtils {
    public static final int CALLBACK_SERVER_PORT = 18980;
    public static final String CALLBACK_ENDPOINT = "/api/auth/callback";
    public static final String REDIRECT_URI = String.format("http://localhost:%d%s", CALLBACK_SERVER_PORT, CALLBACK_ENDPOINT);

    public static final String AUTH0_DOMAIN = "dev-46466kz3hleg0in1.us.auth0.com";
    public static final String CLIENT_ID = "KC2EoCphVVe5wzZj3GyFiTZboulfYJNH";
    public static final String TOKEN_ENDPOINT = String.format("https://%s/oauth/token", AUTH0_DOMAIN);
    public static final String USERINFO_ENDPOINT = String.format("https://%s/userinfo", AUTH0_DOMAIN);
}
