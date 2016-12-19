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
package org.trustedanalytics.scheduler.filesystem;

import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.oauth2.provider.OAuth2Authentication;
import org.springframework.security.oauth2.provider.authentication.OAuth2AuthenticationDetails;
import org.springframework.stereotype.Component;
import org.trustedanalytics.hadoop.config.client.oauth.TapOauthToken;
import org.trustedanalytics.hadoop.kerberos.KrbLoginManager;
import org.trustedanalytics.hadoop.kerberos.KrbLoginManagerFactory;

import javax.security.auth.login.LoginException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

@Component
@Profile("cloud")
public class HdfsFileSystemFactory implements FileSystemFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(HdfsFileSystemFactory.class);

    HdfsConfigProvider hdfsConfigProvider;

    @Autowired
    public HdfsFileSystemFactory(HdfsConfigProvider configProvider)  {
        hdfsConfigProvider = configProvider;
    }

    @Override
    public FileSystem getFileSystem(String org) {
        return getFileSystem(getOAuthToken(), org);
    }

    private static String getOAuthToken() {
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        OAuth2Authentication oauth2 = (OAuth2Authentication) auth;
        OAuth2AuthenticationDetails details = (OAuth2AuthenticationDetails) oauth2.getDetails();
        return details.getTokenValue();
    }

    public FileSystem getFileSystem(String token, String org) {
        try {
            TapOauthToken jwtToken = new TapOauthToken(token);

            if (hdfsConfigProvider.isKerberosEnabled()) {
                KrbLoginManager loginManager =
                        KrbLoginManagerFactory.getInstance().getKrbLoginManagerInstance(hdfsConfigProvider.getKdc(), hdfsConfigProvider.getRealm());
                loginManager.loginInHadoop(loginManager.loginWithJWTtoken(jwtToken), hdfsConfigProvider.getHadoopConf());
            }
            URI uri = new URI(hdfsConfigProvider.getHdfsOrgUri(org));
            return FileSystem.get(uri, hdfsConfigProvider.getHadoopConf(), jwtToken.getUserId());

        } catch (IOException | InterruptedException | URISyntaxException |LoginException e) {
            LOGGER.error("Exception occurred", e);
            throw new IllegalStateException("Error while creating FileSystem object");
        }
    }


}

