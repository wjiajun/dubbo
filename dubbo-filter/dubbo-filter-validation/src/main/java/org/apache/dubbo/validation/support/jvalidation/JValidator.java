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
package org.apache.dubbo.validation.support.jvalidation;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.bytecode.ClassGenerator;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.ReflectUtils;
import org.apache.dubbo.validation.MethodValidated;
import org.apache.dubbo.validation.Validator;

import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtField;
import javassist.CtNewConstructor;
import javassist.Modifier;
import javassist.NotFoundException;
import javassist.bytecode.AnnotationsAttribute;
import javassist.bytecode.ClassFile;
import javassist.bytecode.ConstPool;
import javassist.bytecode.annotation.ArrayMemberValue;
import javassist.bytecode.annotation.BooleanMemberValue;
import javassist.bytecode.annotation.ByteMemberValue;
import javassist.bytecode.annotation.CharMemberValue;
import javassist.bytecode.annotation.ClassMemberValue;
import javassist.bytecode.annotation.DoubleMemberValue;
import javassist.bytecode.annotation.EnumMemberValue;
import javassist.bytecode.annotation.FloatMemberValue;
import javassist.bytecode.annotation.IntegerMemberValue;
import javassist.bytecode.annotation.LongMemberValue;
import javassist.bytecode.annotation.MemberValue;
import javassist.bytecode.annotation.ShortMemberValue;
import javassist.bytecode.annotation.StringMemberValue;

import javax.validation.Constraint;
import javax.validation.ConstraintViolation;
import javax.validation.ConstraintViolationException;
import javax.validation.Validation;
import javax.validation.ValidatorFactory;
import javax.validation.groups.Default;
import java.lang.annotation.Annotation;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Implementation of JValidation. JValidation is invoked if configuration validation attribute value is 'jvalidation'.
 * <pre>
 *     e.g. &lt;dubbo:method name="save" validation="jvalidation" /&gt;
 * </pre>
 */
public class JValidator implements Validator {

    private static final Logger logger = LoggerFactory.getLogger(JValidator.class);

    /**
     * 服务接口类
     */
    private final Class<?> clazz;

    private final Map<String, Class> methodClassMap;

    /**
     * Validator 对象
     */
    private final javax.validation.Validator validator;

    @SuppressWarnings({"unchecked", "rawtypes"})
    public JValidator(URL url) {
        // 获得服务接口类
        this.clazz = ReflectUtils.forName(url.getServiceInterface());
        // 获得 `"jvalidation"` 配置项
        String jvalidation = url.getParameter("jvalidation");
        // 获得 ValidatorFactory 对象
        ValidatorFactory factory;
        if (jvalidation != null && jvalidation.length() > 0) {
            factory = Validation.byProvider((Class) ReflectUtils.forName(jvalidation)).configure().buildValidatorFactory();
        } else {
            factory = Validation.buildDefaultValidatorFactory();
        }
        // 获得 javax Validator 对象
        this.validator = factory.getValidator();
        this.methodClassMap = new ConcurrentHashMap<>();
    }

    /**
      * 使用方法参数，创建 Bean 对象。
      *
      * 因为该 Bean 对象，实际不存在对应类，使用 Javassist 动态编译生成。
      *
      * @param clazz 服务接口类
      * @param method 方法
      * @param args 参数数组
      * @return Bean 对象
      */
    private static Object getMethodParameterBean(Class<?> clazz, Method method, Object[] args) {
        // 无 Constraint 注解的方法参数，无需创建 Bean 对象。
        if (!hasConstraintParameter(method)) {
            return null;
        }
        try {
            // 获得 Bean 类名
            String parameterClassName = generateMethodParameterClassName(clazz, method);
            Class<?> parameterClass;
            try {
                // 获得 Bean 类
                parameterClass = Class.forName(parameterClassName, true, clazz.getClassLoader());
            } catch (ClassNotFoundException e) {
                parameterClass = generateMethodParameterClass(clazz, method, parameterClassName);
            }
            // 创建 Bean 对象
            Object parameterBean = parameterClass.newInstance();
            // 设置 Bean 对象的每个属性的值
            for (int i = 0; i < args.length; i++) {
                Field field = parameterClass.getField(method.getName() + "Argument" + i);
                field.set(parameterBean, args[i]);
            }
            return parameterBean;
        } catch (Throwable e) {
            logger.warn(e.getMessage(), e);
            return null;
        }
    }

    /**
     * try to generate methodParameterClass.
     *
     * @param clazz interface class
     * @param method invoke method
     * @param parameterClassName generated parameterClassName
     * @return Class<?> generated methodParameterClass
     * @throws Exception
     */
    private static Class<?> generateMethodParameterClass(Class<?> clazz, Method method, String parameterClassName)
        throws Exception {
        ClassPool pool = ClassGenerator.getClassPool(clazz.getClassLoader());
        synchronized (parameterClassName.intern()) {
            CtClass ctClass = null;
            try {
                ctClass = pool.getCtClass(parameterClassName);
            } catch (NotFoundException ignore) {
            }

            if (null == ctClass) {
                // 创建 CtClass 对象
                ctClass = pool.makeClass(parameterClassName);
                ClassFile classFile = ctClass.getClassFile();
                // 设置 Java 版本为 5
                classFile.setVersionToJava5();
                // 添加默认构造方法
                ctClass.addConstructor(CtNewConstructor.defaultConstructor(pool.getCtClass(parameterClassName)));
                // 循环每个方法参数，生成对应的类的属性
                // parameter fields
                Class<?>[] parameterTypes = method.getParameterTypes();
                Annotation[][] parameterAnnotations = method.getParameterAnnotations();
                for (int i = 0; i < parameterTypes.length; i++) {
                    Class<?> type = parameterTypes[i];
                    Annotation[] annotations = parameterAnnotations[i];
                    // 创建注解属性
                    AnnotationsAttribute attribute = new AnnotationsAttribute(classFile.getConstPool(), AnnotationsAttribute.visibleTag);
                    // 循环每个方法参数的每个注解
                    for (Annotation annotation : annotations) {
                        if (annotation.annotationType().isAnnotationPresent(Constraint.class)) {
                            javassist.bytecode.annotation.Annotation ja = new javassist.bytecode.annotation.Annotation(
                                classFile.getConstPool(), pool.getCtClass(annotation.annotationType().getName()));
                            // 循环注解的每个方法
                            Method[] members = annotation.annotationType().getMethods();
                            for (Method member : members) {
                                if (Modifier.isPublic(member.getModifiers())
                                    && member.getParameterTypes().length == 0
                                    && member.getDeclaringClass() == annotation.annotationType()) {
                                    // 将注解，添加到类的属性上
                                    Object value = member.invoke(annotation);
                                    if (null != value) {
                                        MemberValue memberValue = createMemberValue(
                                            classFile.getConstPool(), pool.get(member.getReturnType().getName()), value);
                                        ja.addMemberValue(member.getName(), memberValue);
                                    }
                                }
                            }
                            attribute.addAnnotation(ja);
                        }
                    }
                    // 创建属性
                    String fieldName = method.getName() + "Argument" + i;
                    CtField ctField = CtField.make("public " + type.getCanonicalName() + " " + fieldName + ";", pool.getCtClass(parameterClassName));
                    ctField.getFieldInfo().addAttribute(attribute);
                    // 添加属性
                    ctClass.addField(ctField);
                }
                return ctClass.toClass(clazz.getClassLoader(), null);
            } else {
                return Class.forName(parameterClassName, true, clazz.getClassLoader());
            }
        }
    }

    private static String generateMethodParameterClassName(Class<?> clazz, Method method) {
        StringBuilder builder = new StringBuilder().append(clazz.getName())
                .append("_")
                .append(toUpperMethodName(method.getName()))
                .append("Parameter");

        Class<?>[] parameterTypes = method.getParameterTypes();
        for (Class<?> parameterType : parameterTypes) {
            builder.append("_").append(parameterType.getName());
        }

        return builder.toString();
    }

    private static boolean hasConstraintParameter(Method method) {
        Annotation[][] parameterAnnotations = method.getParameterAnnotations();
        // 循环所有方法参数的注解
        if (parameterAnnotations != null && parameterAnnotations.length > 0) {
            // 循环每个方法参数的注解数组
            for (Annotation[] annotations : parameterAnnotations) {
                // 是否有 Constraint 注解
                for (Annotation annotation : annotations) {
                    if (annotation.annotationType().isAnnotationPresent(Constraint.class)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    private static String toUpperMethodName(String methodName) {
        return methodName.substring(0, 1).toUpperCase() + methodName.substring(1);
    }

    // Copy from javassist.bytecode.annotation.Annotation.createMemberValue(ConstPool, CtClass);
    private static MemberValue createMemberValue(ConstPool cp, CtClass type, Object value) throws NotFoundException {
        MemberValue memberValue = javassist.bytecode.annotation.Annotation.createMemberValue(cp, type);
        if (memberValue instanceof BooleanMemberValue) {
            ((BooleanMemberValue) memberValue).setValue((Boolean) value);
        } else if (memberValue instanceof ByteMemberValue) {
            ((ByteMemberValue) memberValue).setValue((Byte) value);
        } else if (memberValue instanceof CharMemberValue) {
            ((CharMemberValue) memberValue).setValue((Character) value);
        } else if (memberValue instanceof ShortMemberValue) {
            ((ShortMemberValue) memberValue).setValue((Short) value);
        } else if (memberValue instanceof IntegerMemberValue) {
            ((IntegerMemberValue) memberValue).setValue((Integer) value);
        } else if (memberValue instanceof LongMemberValue) {
            ((LongMemberValue) memberValue).setValue((Long) value);
        } else if (memberValue instanceof FloatMemberValue) {
            ((FloatMemberValue) memberValue).setValue((Float) value);
        } else if (memberValue instanceof DoubleMemberValue) {
            ((DoubleMemberValue) memberValue).setValue((Double) value);
        } else if (memberValue instanceof ClassMemberValue) {
            ((ClassMemberValue) memberValue).setValue(((Class<?>) value).getName());
        } else if (memberValue instanceof StringMemberValue) {
            ((StringMemberValue) memberValue).setValue((String) value);
        } else if (memberValue instanceof EnumMemberValue) {
            ((EnumMemberValue) memberValue).setValue(((Enum<?>) value).name());
        }
        /* else if (memberValue instanceof AnnotationMemberValue) */
        else if (memberValue instanceof ArrayMemberValue) {
            CtClass arrayType = type.getComponentType();
            int len = Array.getLength(value);
            MemberValue[] members = new MemberValue[len];
            for (int i = 0; i < len; i++) {
                members[i] = createMemberValue(cp, arrayType, Array.get(value, i));
            }
            ((ArrayMemberValue) memberValue).setValue(members);
        }
        return memberValue;
    }

    @Override
    public void validate(String methodName, Class<?>[] parameterTypes, Object[] arguments) throws Exception {
        // 验证分组集合
        List<Class<?>> groups = new ArrayList<>();
        // 【第一种】添加以方法命名的内部接口，作为验证分组。例如 `ValidationService#save(...)` 方法，对应 `ValidationService.Save` 接口。
        Class<?> methodClass = methodClass(methodName);
        if (methodClass != null) {
            groups.add(methodClass);
        }
        Set<ConstraintViolation<?>> violations = new HashSet<>();
        // 【第二种】添加方法的 @MethodValidated 注解的值对应的类，作为验证分组。
        Method method = clazz.getMethod(methodName, parameterTypes);
        Class<?>[] methodClasses;
        if (method.isAnnotationPresent(MethodValidated.class)){
            methodClasses = method.getAnnotation(MethodValidated.class).value();
            groups.addAll(Arrays.asList(methodClasses));
        }
        // add into default group
        // 【第三种】添加 Default.class 类，作为验证分组。在 JSR 303 中，未设置分组的验证注解，使用 Default.class 。
        groups.add(0, Default.class);
        // 【第四种】添加服务接口类，作为验证分组。
        groups.add(1, clazz);

        // convert list to array
        Class<?>[] classgroups = groups.toArray(new Class[groups.size()]);

        // 【第一步】获得方法参数的 Bean 对象。因为，JSR 303 是 Java Bean Validation ，以 Bean 为维度。
        Object parameterBean = getMethodParameterBean(clazz, method, arguments);
        // 【第一步】验证 Bean 对象。
        if (parameterBean != null) {
            // 验证错误集合
            violations.addAll(validator.validate(parameterBean, classgroups ));
        }

        // 【第二步】验证集合参数
        for (Object arg : arguments) {
            validate(violations, arg, classgroups);
        }

        // 若有错误，抛出 ConstraintViolationException 异常。
        if (!violations.isEmpty()) {
            logger.error("Failed to validate service: " + clazz.getName() + ", method: " + methodName + ", cause: " + violations);
            throw new ConstraintViolationException("Failed to validate service: " + clazz.getName() + ", method: " + methodName + ", cause: " + violations, violations);
        }
    }

    private Class methodClass(String methodName) {
        Class<?> methodClass = null;
        String methodClassName = clazz.getName() + "$" + toUpperMethodName(methodName);
        Class cached = methodClassMap.get(methodClassName);
        if (cached != null) {
            return cached == clazz ? null : cached;
        }
        try {
            methodClass = Class.forName(methodClassName, false, Thread.currentThread().getContextClassLoader());
            methodClassMap.put(methodClassName, methodClass);
        } catch (ClassNotFoundException e) {
            methodClassMap.put(methodClassName, clazz);
        }
        return methodClass;
    }

    /**
     * 验证集合参数
     *
     * @param violations 验证错误集合
     * @param arg 参数
     * @param groups 验证分组集合
     */
    private void validate(Set<ConstraintViolation<?>> violations, Object arg, Class<?>... groups) {
        if (arg != null && !ReflectUtils.isPrimitives(arg.getClass())) {
            if (arg instanceof Object[]) {
                for (Object item : (Object[]) arg) {
                    validate(violations, item, groups);
                }
            } else if (arg instanceof Collection) {
                for (Object item : (Collection<?>) arg) {
                    validate(violations, item, groups);
                }
            } else if (arg instanceof Map) {
                for (Map.Entry<?, ?> entry : ((Map<?, ?>) arg).entrySet()) {
                    validate(violations, entry.getKey(), groups);
                    validate(violations, entry.getValue(), groups);
                }
            } else {
                violations.addAll(validator.validate(arg, groups));
            }
        }
    }

}
