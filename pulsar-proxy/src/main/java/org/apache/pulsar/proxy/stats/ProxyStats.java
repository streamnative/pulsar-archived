/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.proxy.stats;

import static java.util.concurrent.TimeUnit.SECONDS;
import io.netty.channel.Channel;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response.Status;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.authentication.AuthenticationParameters;
import org.apache.pulsar.broker.web.AuthenticationFilter;
import org.apache.pulsar.proxy.server.ProxyService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



@Path("/")
@Api(value = "/proxy-stats", description = "Stats for proxy", tags = "proxy-stats", hidden = true)
@Produces(MediaType.APPLICATION_JSON)
public class ProxyStats {

    private static final Logger log = LoggerFactory.getLogger(ProxyStats.class);
    public static final String ATTRIBUTE_PULSAR_PROXY_NAME = "pulsar-proxy";

    private ProxyService service;

    @Context
    protected ServletContext servletContext;
    @Context
    protected HttpServletRequest httpRequest;

    @GET
    @Path("/connections")
    @ApiOperation(value = "Proxy stats api to get info for live connections",
            response = List.class, responseContainer = "List")
    @ApiResponses(value = { @ApiResponse(code = 503, message = "Proxy service is not initialized") })
    public List<ConnectionStats> metrics() {
        throwIfNotSuperUser("metrics");
        List<ConnectionStats> stats = new ArrayList<>();
        proxyService().getClientCnxs().forEach(cnx -> {
            if (cnx.getDirectProxyHandler() == null) {
                return;
            }
            double requestRate = cnx.getDirectProxyHandler().getInboundChannelRequestsRate().getRate();
            double byteRate = cnx.getDirectProxyHandler().getInboundChannelRequestsRate().getValueRate();
            Channel inboundChannel = cnx.getDirectProxyHandler().getInboundChannel();
            Channel outboundChannel = cnx.getDirectProxyHandler().getOutboundChannel();
            stats.add(new ConnectionStats(requestRate, byteRate, inboundChannel, outboundChannel));
        });
        return stats;
    }

    @GET
    @Path("/topics")
    @ApiOperation(value = "Proxy topic stats api", response = Map.class, responseContainer = "Map")
    @ApiResponses(value = { @ApiResponse(code = 412, message = "Proxy logging should be > 2 to capture topic stats"),
            @ApiResponse(code = 503, message = "Proxy service is not initialized") })
    public Map<String, TopicStats> topics() {
        throwIfNotSuperUser("topics");
        Optional<Integer> logLevel = proxyService().getConfiguration().getProxyLogLevel();
        if (!logLevel.isPresent() || logLevel.get() < 2) {
            throw new RestException(Status.PRECONDITION_FAILED, "Proxy doesn't have logging level 2");
        }
        return proxyService().getTopicStats();
    }

    @POST
    @Path("/logging/{logLevel}")
    @ApiOperation(hidden = true, value = "Change proxy logging level dynamically",
            notes = "It only changes log-level in memory, change it config file to persist the change")
    @ApiResponses(value = { @ApiResponse(code = 412, message = "Proxy log level can be [0-2]"), })
    public void updateProxyLogLevel(@PathParam("logLevel") int logLevel) {
        throwIfNotSuperUser("updateProxyLogLevel");
        if (logLevel < 0 || logLevel > 2) {
            throw new RestException(Status.PRECONDITION_FAILED, "Proxy log level can be only [0-2]");
        }
        proxyService().setProxyLogLevel(logLevel);
    }

    @GET
    @Path("/logging")
    @ApiOperation(hidden = true, value = "Get proxy logging")
    public int getProxyLogLevel(@PathParam("logLevel") int logLevel) {
        throwIfNotSuperUser("getProxyLogLevel");
        return proxyService().getProxyLogLevel();
    }

    protected ProxyService proxyService() {
        if (service == null) {
            service = (ProxyService) servletContext.getAttribute(ATTRIBUTE_PULSAR_PROXY_NAME);
            if (service == null) {
                throw new RestException(Status.SERVICE_UNAVAILABLE, "Proxy service is not initialized");
            }
        }
        return service;
    }

    private void throwIfNotSuperUser(String action) {
        if (proxyService().getConfiguration().isAuthorizationEnabled()) {
            AuthenticationParameters authParams = AuthenticationParameters.builder()
                    .clientRole((String) httpRequest.getAttribute(AuthenticationFilter.AuthenticatedRoleAttributeName))
                    .clientAuthenticationDataSource((AuthenticationDataSource)
                            httpRequest.getAttribute(AuthenticationFilter.AuthenticatedDataAttributeName))
                    .build();
            try {
                if (authParams.getClientRole() == null
                        || !proxyService().getAuthorizationService().isSuperUser(authParams).get(30, SECONDS)) {
                    log.error("Client with role [{}] is not authorized to {}", authParams.getClientRole(), action);
                    throw new org.apache.pulsar.common.util.RestException(Status.UNAUTHORIZED,
                            "Client is not authorized to perform operation");
                }
            } catch (ExecutionException | TimeoutException | InterruptedException e) {
                log.warn("Time-out {} sec while checking the role {} is a super user role ", 30,
                        authParams.getClientRole());
                throw new org.apache.pulsar.common.util.RestException(Status.INTERNAL_SERVER_ERROR, e.getMessage());
            }
        }
    }
}
