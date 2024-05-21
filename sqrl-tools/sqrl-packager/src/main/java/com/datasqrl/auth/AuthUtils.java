package com.datasqrl.auth;

public class AuthUtils {

  public static final int CALLBACK_SERVER_PORT = 18980;
  public static final String CALLBACK_ENDPOINT = "/api/auth/callback";
  public static final String REDIRECT_URI = String.format("http://localhost:%d%s", CALLBACK_SERVER_PORT, CALLBACK_ENDPOINT);

  public static final String CLIENT_ID = "Ov9VIEZyzsxRsnKRcgbSwOsXfc6WUBUU";
  public static final String AUDIENCE = "https://dev.datasqrl.com";
  public static final String AUTH0_BASE_URL = "https://dev.datasqrl.com/auth0";
  public static final String TOKEN_ENDPOINT = AUTH0_BASE_URL + "/oauth/token";
  public static final String AUTHORIZE_ENDPOINT = AUTH0_BASE_URL + "/authorize";
}
