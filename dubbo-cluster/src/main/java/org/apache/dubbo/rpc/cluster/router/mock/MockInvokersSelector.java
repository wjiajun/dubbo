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
package org.apache.dubbo.rpc.cluster.router.mock;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.cluster.router.AbstractRouter;

import java.util.ArrayList;
import java.util.List;

import static org.apache.dubbo.rpc.cluster.Constants.INVOCATION_NEED_MOCK;
import static org.apache.dubbo.rpc.cluster.Constants.MOCK_PROTOCOL;

/**
 * A specific Router designed to realize mock feature.
 * If a request is configured to use mock, then this router guarantees that only the invokers with protocol MOCK appear in final the invoker list, all other invokers will be excluded.
 */
public class MockInvokersSelector extends AbstractRouter {

    public static final String NAME = "MOCK_ROUTER";
    private static final int MOCK_INVOKERS_DEFAULT_PRIORITY = -100;

    public MockInvokersSelector() {
        this.priority = MOCK_INVOKERS_DEFAULT_PRIORITY;
    }

    @Override
    public <T> List<Invoker<T>> route(final List<Invoker<T>> invokers,
                                      URL url, final Invocation invocation) throws RpcException {
        // 获得普通 Invoker 集合
        if (CollectionUtils.isEmpty(invokers)) {
            return invokers;
        }

        if (invocation.getObjectAttachments() == null) {
            return getNormalInvokers(invokers);
        } else {
            // 获得 "invocation.need.mock" 配置项
            String value = (String) invocation.getObjectAttachments().get(INVOCATION_NEED_MOCK);
            // 获得普通 Invoker 集合
            if (value == null) {
                return getNormalInvokers(invokers);
            // 获得 MockInvoker 集合
            } else if (Boolean.TRUE.toString().equalsIgnoreCase(value)) {
                return getMockedInvokers(invokers);
            }
        }
        // 其它，不匹配，直接返回 `invokers` 集合
        return invokers;
    }

    private <T> List<Invoker<T>> getMockedInvokers(final List<Invoker<T>> invokers) {
        // 不包含 MockInvoker 的情况下，直接返回 null
        if (!hasMockProviders(invokers)) {
            return null;
        }
        // 过滤掉普通 kInvoker ，创建 MockInvoker 集合
        List<Invoker<T>> sInvokers = new ArrayList<Invoker<T>>(1);
        for (Invoker<T> invoker : invokers) {
            if (invoker.getUrl().getProtocol().equals(MOCK_PROTOCOL)) {
                sInvokers.add(invoker);
            }
        }
        return sInvokers;
    }

    private <T> List<Invoker<T>> getNormalInvokers(final List<Invoker<T>> invokers) {
        // 不包含 MockInvoker 的情况下，直接返回 `invokers` 集合
        if (!hasMockProviders(invokers)) {
            return invokers;
        } else {
            // 若包含 MockInvoker 的情况下，过滤掉 MockInvoker ，创建普通 Invoker 集合
            List<Invoker<T>> sInvokers = new ArrayList<Invoker<T>>(invokers.size());
            for (Invoker<T> invoker : invokers) {
                if (!invoker.getUrl().getProtocol().equals(MOCK_PROTOCOL)) {
                    sInvokers.add(invoker);
                }
            }
            return sInvokers;
        }
    }

    private <T> boolean hasMockProviders(final List<Invoker<T>> invokers) {
        boolean hasMockProvider = false;
        for (Invoker<T> invoker : invokers) {
            if (invoker.getUrl().getProtocol().equals(MOCK_PROTOCOL)) {
                hasMockProvider = true;
                break;
            }
        }
        return hasMockProvider;
    }

}
