package io.odpf.depot.config;


public interface HttpClientConfig extends OdpfSinkConfig {

    @Key("HTTP_MAX_CONNECTIONS")
    @DefaultValue("10")
    Integer getHttpMaxConnections();

    @Key("HTTP_REQUEST_TIMEOUT_MS")
    @DefaultValue("10000")
    Integer getHttpRequestTimeoutMs();

    @Key("HTTP_OAUTH2_ENABLE")
    @DefaultValue("false")
    Boolean isHttpOAuth2Enable();

    @Key("HTTP_OAUTH2_ACCESS_TOKEN_URL")
    @DefaultValue("https://localhost:8888")
    String getHttpOAuth2AccessTokenUrl();

    @Key("HTTP_OAUTH2_CLIENT_NAME")
    @DefaultValue("client_name")
    String getHttpOAuth2ClientName();

    @Key("HTTP_OAUTH2_CLIENT_SECRET")
    @DefaultValue("client_secret")
    String getHttpOAuth2ClientSecret();

    @Key("HTTP_OAUTH2_SCOPE")
    @DefaultValue("scope")
    String getHttpOAuth2Scope();
}
