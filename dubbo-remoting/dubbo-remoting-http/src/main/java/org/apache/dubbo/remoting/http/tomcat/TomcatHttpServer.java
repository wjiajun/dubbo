/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.dubbo.remoting.http.tomcat;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.remoting.http.HttpHandler;
import org.apache.dubbo.remoting.http.servlet.DispatcherServlet;
import org.apache.dubbo.remoting.http.servlet.ServletManager;
import org.apache.dubbo.remoting.http.support.AbstractHttpServer;

import org.apache.catalina.Context;
import org.apache.catalina.LifecycleException;
import org.apache.catalina.connector.Connector;
import org.apache.catalina.startup.Tomcat;

import java.io.File;

import static org.apache.dubbo.common.constants.CommonConstants.THREADS_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.DEFAULT_THREADS;
import static org.apache.dubbo.remoting.Constants.ACCEPTS_KEY;

public class TomcatHttpServer extends AbstractHttpServer {

    private static final Logger logger = LoggerFactory.getLogger(TomcatHttpServer.class);

    /**
      * 内嵌的 Tomcat 对象
      */
    private final Tomcat tomcat;

    private final URL url;

    public TomcatHttpServer(URL url, final HttpHandler handler) {
        super(url, handler);

        this.url = url;
        // 注册 HttpHandler 到 DispatcherServlet 中
        DispatcherServlet.addHttpHandler(url.getPort(), handler);
        // 创建内嵌的 Tomcat 对象
        String baseDir = new File(System.getProperty("java.io.tmpdir")).getAbsolutePath();
        tomcat = new Tomcat();

        Connector connector = new Connector("org.apache.coyote.http11.Http11NioProtocol");
        connector.setPort(url.getPort());
        connector.setProperty("maxThreads", String.valueOf(url.getParameter(THREADS_KEY, DEFAULT_THREADS)));
        connector.setProperty("maxConnections", String.valueOf(url.getParameter(ACCEPTS_KEY, -1)));
        connector.setProperty("URIEncoding", "UTF-8");
        connector.setProperty("connectionTimeout", "60000");
        connector.setProperty("maxKeepAliveRequests", "-1");
        tomcat.setConnector(connector);

        tomcat.setBaseDir(baseDir);
        tomcat.setPort(url.getPort());

        // 添加 DispatcherServlet 到 Tomcat 中
        Context context = tomcat.addContext("/", baseDir);
        Tomcat.addServlet(context, "dispatcher", new DispatcherServlet());
        context.addServletMapping("/*", "dispatcher");
        // 添加 ServletContext 对象，到 ServletManager 中
        ServletManager.getInstance().addServletContext(url.getPort(), context.getServletContext());

        // tell tomcat to fail on startup failures.
        System.setProperty("org.apache.catalina.startup.EXIT_ON_INIT_FAILURE", "true");

        try {
            // 启动 Tomcat
            tomcat.start();
        } catch (LifecycleException e) {
            throw new IllegalStateException("Failed to start tomcat server at " + url.getAddress(), e);
        }
    }

    @Override
    public void close() {
        // 标记关闭
        super.close();

        // 移除 ServletContext 对象
        ServletManager.getInstance().removeServletContext(url.getPort());

        // 关闭 Tomcat
        try {
            tomcat.stop();
        } catch (Exception e) {
            logger.warn(e.getMessage(), e);
        }
    }
}
