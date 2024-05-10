package com.datasqrl.auth;

public class AuthUtils {

  public static final int CALLBACK_SERVER_PORT = 18980;
  public static final String CALLBACK_ENDPOINT = "/api/auth/callback";
  public static final String REDIRECT_URI = String.format("http://localhost:%d%s", CALLBACK_SERVER_PORT, CALLBACK_ENDPOINT);

  public static final String CLIENT_ID = "KC2EoCphVVe5wzZj3GyFiTZboulfYJNH";
  public static final String AUDIENCE = "https://dev-46466kz3hleg0in1.us.auth0.com/api/v2/";
  public static final String AUTH0_BASE_URL = "https://sqrl-repository-frontend-git-staging-datasqrl.vercel.app/auth0";
  public static final String TOKEN_ENDPOINT = AUTH0_BASE_URL + "/oauth/token";
  public static final String AUTHORIZE_ENDPOINT = AUTH0_BASE_URL + "/authorize";
}
