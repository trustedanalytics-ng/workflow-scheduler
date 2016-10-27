/**
 * Copyright (c) 2016 Intel Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.trustedanalytics.scheduler.rest;



import org.apache.http.auth.AuthSchemeProvider;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.AuthSchemes;
import org.apache.http.config.Lookup;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.auth.SPNegoSchemeFactory;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.kerberos.client.KerberosRestTemplate;
import org.springframework.security.oauth2.provider.OAuth2Authentication;
import org.springframework.security.oauth2.provider.authentication.OAuth2AuthenticationDetails;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;
import org.trustedanalytics.hadoop.config.client.oauth.TapOauthToken;
import org.trustedanalytics.hadoop.kerberos.KrbLoginManager;
import org.trustedanalytics.hadoop.kerberos.KrbLoginManagerFactory;
import org.trustedanalytics.scheduler.config.ClouderaConfiguration;
import org.trustedanalytics.scheduler.filesystem.HdfsConfigProvider;
import sun.security.krb5.PrincipalName;

import javax.net.ssl.SSLContext;
import javax.security.auth.login.LoginException;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.HashMap;
import java.util.Map;

@Component
public class RestOperationsFactory {

    private static final String KRB5_CREDENTIALS_CACHE_DIR = "/tmp/";
    private static final Logger LOGGER = LoggerFactory.getLogger(RestOperationsFactory.class);
    private HdfsConfigProvider hdfsConfigProvider;

    private final ClouderaConfiguration configuration;

    private final SSLContext sslContext;

    @Autowired
    public RestOperationsFactory(HdfsConfigProvider configProvider, ClouderaConfiguration configuration, SSLContext sslContext) throws IOException, GeneralSecurityException {
        this.configuration = configuration;
        hdfsConfigProvider = configProvider;
        this.sslContext = sslContext;
    }

    static String ticketCacheLocation(String princName) {
        return KRB5_CREDENTIALS_CACHE_DIR
                + princName.replace(PrincipalName.NAME_COMPONENT_SEPARATOR, '_');
    }
    private static String getOAuthToken() {
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        OAuth2Authentication oauth2 = (OAuth2Authentication) auth;
        OAuth2AuthenticationDetails details = (OAuth2AuthenticationDetails) oauth2.getDetails();
        return details.getTokenValue();
    }

    public RestTemplate getRestTemplate()  {
        if (hdfsConfigProvider.isKerberosEnabled()) {
            try {
                KrbLoginManager loginManager =
                        KrbLoginManagerFactory.getInstance().getKrbLoginManagerInstance(hdfsConfigProvider.getKdc(), hdfsConfigProvider.getRealm());
                TapOauthToken jwtToken = new TapOauthToken(getOAuthToken());
                loginManager.loginWithJWTtoken(jwtToken);

                return createKerberosRestTemplate(jwtToken.getUserId());
            } catch (LoginException e) {
                LOGGER.error("Kerberos login exception", e);
                throw new IllegalStateException("Unable to authenticate in kerberos");
            }
        } else {
            LOGGER.warn("No valid Kerberos configuration detected, creating standard RestTemplate");
            return createRestTemplate();
        }
    }

    private RestTemplate createRestTemplate() {
        BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(configuration.getUser(), configuration.getPassword()));

        SSLConnectionSocketFactory factory = new SSLConnectionSocketFactory(sslContext);

        HttpClient httpClient = HttpClientBuilder.create().setSSLSocketFactory(factory)
                .setDefaultCredentialsProvider(credentialsProvider).build();

        return new RestTemplate(new HttpComponentsClientHttpRequestFactory(httpClient));
    }

    private RestTemplate createKerberosRestTemplate(String userId) {

        Map<String, Object> options = new HashMap<>();
        options.put("ticketCache", ticketCacheLocation(userId + PrincipalName.NAME_REALM_SEPARATOR_STR
                + System.getProperty(HdfsConfigProvider.KRB5_REALM)));
        RestTemplate template =  new KerberosRestTemplate("", userId, options);

        BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(configuration.getUser(), configuration.getPassword()));
        HttpClientBuilder builder = HttpClientBuilder.create();
        Lookup<AuthSchemeProvider> authSchemeRegistry = RegistryBuilder.<AuthSchemeProvider> create()
                .register(AuthSchemes.SPNEGO, new SPNegoSchemeFactory(true)).build();
        builder.setDefaultAuthSchemeRegistry(authSchemeRegistry);
        builder.setDefaultCredentialsProvider(credentialsProvider);

        SSLConnectionSocketFactory factory = new SSLConnectionSocketFactory(sslContext);
        builder.setSSLSocketFactory(factory);
        CloseableHttpClient httpClient = builder.build();

        HttpComponentsClientHttpRequestFactory requestFactory = new HttpComponentsClientHttpRequestFactory(httpClient);
        template.setRequestFactory(requestFactory);
        return template;
    }

}
