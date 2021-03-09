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
package org.apache.dubbo.cache.support.jcache;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.utils.StringUtils;

import javax.cache.Cache;
import javax.cache.CacheException;
import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import javax.cache.spi.CachingProvider;
import java.util.concurrent.TimeUnit;

import static org.apache.dubbo.common.constants.CommonConstants.METHOD_KEY;

/**
 * This class store the cache value per thread. If a service,method,consumer or provided is configured with key <b>cache</b>
 * with value <b>jcache</b>, dubbo initialize the instance of this class using {@link JCacheFactory} to store method's returns value
 * to server from store without making method call.
 *
 * @see Cache
 * @see JCacheFactory
 * @see org.apache.dubbo.cache.support.AbstractCacheFactory
 * @see org.apache.dubbo.cache.filter.CacheFilter
 */
public class JCache implements org.apache.dubbo.cache.Cache {

    private final Cache<Object, Object> store;

    public JCache(URL url) {
        // 获得 Cache Key
        String method = url.getParameter(METHOD_KEY, "");
        String key = url.getAddress() + "." + url.getServiceKey() + "." + method;
        // jcache parameter is the full-qualified class name of SPI implementation
        // `"jcache"` 配置项，为 Java SPI 实现的全限定类名
        String type = url.getParameter("jcache");

        // 基于类型，获得 javax.cache.CachingProvider 对象，
        CachingProvider provider = StringUtils.isEmpty(type) ? Caching.getCachingProvider() : Caching.getCachingProvider(type);
        // 获得 javax.cache.CacheManager 对象
        CacheManager cacheManager = provider.getCacheManager();
        // 获得 javax.cache.Cache 对象
        Cache<Object, Object> cache = cacheManager.getCache(key);
        if (cache == null) {
            try {
                //configure the cache
                // 设置 Cache 配置项
                MutableConfiguration config =
                        new MutableConfiguration<>()
                                // 类型
                                .setTypes(Object.class, Object.class)
                                // 过期策略，按照写入时间过期。通过 `"cache.write.expire"` 配置项设置过期时间，默认为 1 分钟。
                                .setExpiryPolicyFactory(CreatedExpiryPolicy.factoryOf(new Duration(TimeUnit.MILLISECONDS, url.getMethodParameter(method, "cache.write.expire", 60 * 1000))))
                                .setStoreByValue(false)
                                // 设置 MBean
                                .setManagementEnabled(true)
                                .setStatisticsEnabled(true);
                cache = cacheManager.createCache(key, config);
            } catch (CacheException e) {
                // concurrent cache initialization
                cache = cacheManager.getCache(key);
            }
        }

        this.store = cache;
    }

    @Override
    public void put(Object key, Object value) {
        store.put(key, value);
    }

    @Override
    public Object get(Object key) {
        return store.get(key);
    }

}
