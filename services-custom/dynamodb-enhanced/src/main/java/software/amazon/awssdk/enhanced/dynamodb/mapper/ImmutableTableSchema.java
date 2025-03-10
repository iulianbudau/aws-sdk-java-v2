/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package software.amazon.awssdk.enhanced.dynamodb.mapper;

import static software.amazon.awssdk.enhanced.dynamodb.internal.DynamoDbEnhancedLogger.BEAN_LOGGER;

import java.lang.annotation.Annotation;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.WeakHashMap;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import software.amazon.awssdk.annotations.SdkPublicApi;
import software.amazon.awssdk.annotations.SdkTestInternalApi;
import software.amazon.awssdk.annotations.ThreadSafe;
import software.amazon.awssdk.enhanced.dynamodb.AttributeConverter;
import software.amazon.awssdk.enhanced.dynamodb.AttributeConverterProvider;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.EnhancedType;
import software.amazon.awssdk.enhanced.dynamodb.EnhancedTypeDocumentConfiguration;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.enhanced.dynamodb.internal.AttributeConfiguration;
import software.amazon.awssdk.enhanced.dynamodb.internal.immutable.ImmutableInfo;
import software.amazon.awssdk.enhanced.dynamodb.internal.immutable.ImmutableIntrospector;
import software.amazon.awssdk.enhanced.dynamodb.internal.immutable.ImmutablePropertyDescriptor;
import software.amazon.awssdk.enhanced.dynamodb.internal.mapper.BeanAttributeGetter;
import software.amazon.awssdk.enhanced.dynamodb.internal.mapper.BeanAttributeSetter;
import software.amazon.awssdk.enhanced.dynamodb.internal.mapper.MetaTableSchema;
import software.amazon.awssdk.enhanced.dynamodb.internal.mapper.MetaTableSchemaCache;
import software.amazon.awssdk.enhanced.dynamodb.internal.mapper.ObjectConstructor;
import software.amazon.awssdk.enhanced.dynamodb.internal.mapper.ObjectGetterMethod;
import software.amazon.awssdk.enhanced.dynamodb.internal.mapper.StaticGetterMethod;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.BeanTableSchemaAttributeTag;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbAttribute;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbConvertedBy;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbFlatten;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbIgnoreNulls;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbImmutable;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbPreserveEmptyObject;

/**
 * Implementation of {@link TableSchema} that builds a table schema based on properties and annotations of an immutable
 * class with an associated builder class. Example:
 * <pre>
 * <code>
 * {@literal @}DynamoDbImmutable(builder = Customer.Builder.class)
 * public class Customer {
 *     {@literal @}DynamoDbPartitionKey
 *     public String accountId() { ... }
 *
 *     {@literal @}DynamoDbSortKey
 *     public int subId() { ... }
 *
 *     // Defines a GSI (customers_by_name) with a partition key of 'name'
 *     {@literal @}DynamoDbSecondaryPartitionKey(indexNames = "customers_by_name")
 *     public String name() { ... }
 *
 *     // Defines an LSI (customers_by_date) with a sort key of 'createdDate' and also declares the
 *     // same attribute as a sort key for the GSI named 'customers_by_name'
 *     {@literal @}DynamoDbSecondarySortKey(indexNames = {"customers_by_date", "customers_by_name"})
 *     public Instant createdDate() { ... }
 *
 *     // Not required to be an inner-class, but builders often are
 *     public static final class Builder {
 *         public Builder accountId(String accountId) { ... };
 *         public Builder subId(int subId) { ... };
 *         public Builder name(String name) { ... };
 *         public Builder createdDate(Instant createdDate) { ... };
 *
 *         public Customer build() { ... };
 *     }
 * }
 * </pre>
 *
 * Creating an {@link ImmutableTableSchema} is a moderately expensive operation, and should be performed sparingly. This is
 * usually done once at application startup.
 *
 * If this table schema is not behaving as you expect, enable debug logging for 'software.amazon.awssdk.enhanced.dynamodb.beans'.
 *
 * @param <T> The type of object that this {@link TableSchema} maps to.
 */
@SdkPublicApi
@ThreadSafe
public final class ImmutableTableSchema<T> extends WrappedTableSchema<T, StaticImmutableTableSchema<T, ?>> {
    private static final String ATTRIBUTE_TAG_STATIC_SUPPLIER_NAME = "attributeTagFor";
    private static final Map<Class<?>, ImmutableTableSchema<?>> IMMUTABLE_TABLE_SCHEMA_CACHE =
        Collections.synchronizedMap(new WeakHashMap<>());

    private ImmutableTableSchema(StaticImmutableTableSchema<T, ?> wrappedTableSchema) {
        super(wrappedTableSchema);
    }

    /**
     * Scans an immutable class and builds an {@link ImmutableTableSchema} from it that can be used with the
     * {@link DynamoDbEnhancedClient}.
     * <p>
     * Creating an {@link ImmutableTableSchema} is a moderately expensive operation, and should be performed sparingly. This is
     * usually done once at application startup.
     * <p>
     * Generally, this method should be preferred over {@link #create(Class)} because it allows you to use a custom
     * {@link MethodHandles.Lookup} instance, which is necessary when your application runs in an environment where your
     * application code and dependencies like the AWS SDK for Java are loaded by different classloaders.
     *
     * @param params The parameters object.
     * @param <T> The immutable class type.
     * @return An initialized {@link ImmutableTableSchema}
     */
    @SuppressWarnings("unchecked")
    public static <T> ImmutableTableSchema<T> create(ImmutableTableSchemaParams<T> params) {
        return (ImmutableTableSchema<T>) IMMUTABLE_TABLE_SCHEMA_CACHE.computeIfAbsent(
            params.immutableClass(), clz -> create(params, new MetaTableSchemaCache()));
    }

    /**
     * Scans an immutable class and builds an {@link ImmutableTableSchema} from it that can be used with the
     * {@link DynamoDbEnhancedClient}.
     * <p>
     * Creating an {@link ImmutableTableSchema} is a moderately expensive operation, and should be performed sparingly. This is
     * usually done once at application startup.
     * <p>
     * If you are running your application in an environment where {@code beanClass} and the SDK are loaded by different
     * classloaders, you should consider using the {@link #create(ImmutableTableSchemaParams)} overload instead, and provided a
     * custom {@link MethodHandles.Lookup} object to ensure that the SDK has access to the {@code beanClass} and its properties
     * at runtime.
     *
     * @param immutableClass The annotated immutable class to build the table schema from.
     * @param <T> The immutable class type.
     * @return An initialized {@link ImmutableTableSchema}
     */
    @SuppressWarnings("unchecked")
    public static <T> ImmutableTableSchema<T> create(Class<T> immutableClass) {
        return create(ImmutableTableSchemaParams.builder(immutableClass).build());
    }

    private static <T> ImmutableTableSchema<T> create(ImmutableTableSchemaParams<T> params,
                                                      MetaTableSchemaCache metaTableSchemaCache) {
        debugLog(params.immutableClass(), () -> "Creating immutable schema");

        // Fetch or create a new reference to this yet-to-be-created TableSchema in the cache
        MetaTableSchema<T> metaTableSchema = metaTableSchemaCache.getOrCreate(params.immutableClass());

        ImmutableTableSchema<T> newTableSchema =
            new ImmutableTableSchema<>(createStaticImmutableTableSchema(params.immutableClass(),
                                                                        params.lookup(),
                                                                        metaTableSchemaCache));
        metaTableSchema.initialize(newTableSchema);
        return newTableSchema;
    }

    // Called when creating an immutable TableSchema recursively. Utilizes the MetaTableSchema cache to stop infinite
    // recursion
    static <T> TableSchema<T> recursiveCreate(Class<T> immutableClass, MethodHandles.Lookup lookup,
                                              MetaTableSchemaCache metaTableSchemaCache) {
        Optional<MetaTableSchema<T>> metaTableSchema = metaTableSchemaCache.get(immutableClass);

        // If we get a cache hit...
        if (metaTableSchema.isPresent()) {
            // Either: use the cached concrete TableSchema if we have one
            if (metaTableSchema.get().isInitialized()) {
                return metaTableSchema.get().concreteTableSchema();
            }

            // Or: return the uninitialized MetaTableSchema as this must be a recursive reference and it will be
            // initialized later as the chain completes
            return metaTableSchema.get();
        }

        // Otherwise: cache doesn't know about this class; create a new one from scratch
        return create(ImmutableTableSchemaParams.builder(immutableClass).lookup(lookup).build(), metaTableSchemaCache);

    }

    private static <T> StaticImmutableTableSchema<T, ?> createStaticImmutableTableSchema(
            Class<T> immutableClass, MethodHandles.Lookup lookup, MetaTableSchemaCache metaTableSchemaCache) {
        ImmutableInfo<T> immutableInfo = ImmutableIntrospector.getImmutableInfo(immutableClass);
        Class<?> builderClass = immutableInfo.builderClass();
        return createStaticImmutableTableSchema(immutableClass, builderClass, immutableInfo, lookup, metaTableSchemaCache);
    }

    private static <T, B> StaticImmutableTableSchema<T, B>  createStaticImmutableTableSchema(
        Class<T> immutableClass,
        Class<B> builderClass,
        ImmutableInfo<T> immutableInfo,
        MethodHandles.Lookup lookup,
        MetaTableSchemaCache metaTableSchemaCache) {

        Supplier<B> newBuilderSupplier = newObjectSupplier(immutableInfo, builderClass, lookup);
        Function<B, T> buildFunction = ObjectGetterMethod.create(builderClass, immutableInfo.buildMethod(), lookup);

        StaticImmutableTableSchema.Builder<T, B> builder =
            StaticImmutableTableSchema.builder(immutableClass, builderClass)
                                      .newItemBuilder(newBuilderSupplier, buildFunction);

        builder.attributeConverterProviders(
            createConverterProvidersFromAnnotation(immutableClass, lookup,
                                                   immutableClass.getAnnotation(DynamoDbImmutable.class)));

        List<ImmutableAttribute<T, B, ?>> attributes = new ArrayList<>();

        immutableInfo.propertyDescriptors()
              .forEach(propertyDescriptor -> {
                  DynamoDbFlatten dynamoDbFlatten = getPropertyAnnotation(propertyDescriptor, DynamoDbFlatten.class);

                  if (dynamoDbFlatten != null) {
                      builder.flatten(TableSchema.fromClass(propertyDescriptor.getter().getReturnType()),
                                      getterForProperty(propertyDescriptor, immutableClass, lookup),
                                      setterForProperty(propertyDescriptor, builderClass, lookup));
                  } else {
                      AttributeConfiguration beanAttributeConfiguration = resolveAttributeConfiguration(propertyDescriptor);
                      ImmutableAttribute.Builder<T, B, ?> attributeBuilder =
                          immutableAttributeBuilder(propertyDescriptor,
                                                    immutableClass,
                                                    builderClass,
                                                    lookup,
                                                    metaTableSchemaCache,
                                                    beanAttributeConfiguration);

                      Optional<AttributeConverter> attributeConverter =
                              createAttributeConverterFromAnnotation(propertyDescriptor, lookup);
                      attributeConverter.ifPresent(attributeBuilder::attributeConverter);

                      addTagsToAttribute(attributeBuilder, propertyDescriptor);
                      attributes.add(attributeBuilder.build());
                  }
              });

        builder.attributes(attributes);

        return builder.build();
    }

    private static List<AttributeConverterProvider> createConverterProvidersFromAnnotation(Class<?> immutableClass,
                                                                                           MethodHandles.Lookup lookup,
                                                                                           DynamoDbImmutable dynamoDbImmutable) {

        Class<? extends AttributeConverterProvider>[] providerClasses = dynamoDbImmutable.converterProviders();

        return Arrays.stream(providerClasses)
                     .peek(c -> debugLog(immutableClass, () -> "Adding Converter: " + c.getTypeName()))
                     .map(c -> (AttributeConverterProvider) newObjectSupplierForClass(c, lookup).get())
                     .collect(Collectors.toList());
    }

    private static <T, B> ImmutableAttribute.Builder<T, B, ?> immutableAttributeBuilder(
        ImmutablePropertyDescriptor propertyDescriptor,
        Class<T> immutableClass, Class<B> builderClass,
        MethodHandles.Lookup lookup,
        MetaTableSchemaCache metaTableSchemaCache,
        AttributeConfiguration beanAttributeConfiguration) {

        Type propertyType = propertyDescriptor.getter().getGenericReturnType();
        EnhancedType<?> propertyTypeToken = convertTypeToEnhancedType(propertyType,
                                                                      lookup,
                                                                      metaTableSchemaCache,
                                                                      beanAttributeConfiguration);
        return ImmutableAttribute.builder(immutableClass, builderClass, propertyTypeToken)
                                 .name(attributeNameForProperty(propertyDescriptor))
                                 .getter(getterForProperty(propertyDescriptor, immutableClass, lookup))
                                 .setter(setterForProperty(propertyDescriptor, builderClass, lookup));
    }

    /**
     * Converts a {@link Type} to an {@link EnhancedType}. Usually {@link EnhancedType#of} is capable of doing this all
     * by itself, but for the ImmutableTableSchema we want to detect if a parameterized class is being passed without a
     * converter that is actually another annotated class in which case we want to capture its schema and add it to the
     * EnhancedType. Unfortunately this means we have to duplicate some of the recursive Type parsing that
     * EnhancedClient otherwise does all by itself.
     */
    @SuppressWarnings("unchecked")
    private static EnhancedType<?> convertTypeToEnhancedType(Type type,
                                                             MethodHandles.Lookup lookup,
                                                             MetaTableSchemaCache metaTableSchemaCache,
                                                             AttributeConfiguration attributeConfiguration) {
        Class<?> clazz = null;

        if (type instanceof ParameterizedType) {
            ParameterizedType parameterizedType = (ParameterizedType) type;
            Type rawType = parameterizedType.getRawType();

            if (List.class.equals(rawType)) {
                EnhancedType<?> enhancedType = convertTypeToEnhancedType(parameterizedType.getActualTypeArguments()[0], lookup,
                                                                         metaTableSchemaCache, attributeConfiguration);
                return EnhancedType.listOf(enhancedType);
            }

            if (Map.class.equals(rawType)) {
                EnhancedType<?> enhancedType = convertTypeToEnhancedType(parameterizedType.getActualTypeArguments()[1], lookup,
                                                                         metaTableSchemaCache, attributeConfiguration);
                return EnhancedType.mapOf(EnhancedType.of(parameterizedType.getActualTypeArguments()[0]),
                                          enhancedType);
            }

            if (rawType instanceof Class) {
                clazz = (Class<?>) rawType;
            }
        } else if (type instanceof Class) {
            clazz = (Class<?>) type;
        }

        if (clazz != null) {
            Consumer<EnhancedTypeDocumentConfiguration.Builder> attrConfiguration =
                b -> b.preserveEmptyObject(attributeConfiguration.preserveEmptyObject())
                      .ignoreNulls(attributeConfiguration.ignoreNulls());
            if (clazz.getAnnotation(DynamoDbImmutable.class) != null) {
                return EnhancedType.documentOf(
                    (Class<Object>) clazz,
                    (TableSchema<Object>) ImmutableTableSchema.recursiveCreate(clazz,
                                                                               lookup,
                                                                               metaTableSchemaCache),
                    attrConfiguration);
            } else if (clazz.getAnnotation(DynamoDbBean.class) != null) {
                return EnhancedType.documentOf(
                    (Class<Object>) clazz,
                    (TableSchema<Object>) BeanTableSchema.recursiveCreate(clazz,
                                                                          lookup,
                                                                          metaTableSchemaCache),
                    attrConfiguration);
            }
        }

        return EnhancedType.of(type);
    }

    private static Optional<AttributeConverter> createAttributeConverterFromAnnotation(
            ImmutablePropertyDescriptor propertyDescriptor, MethodHandles.Lookup lookup) {
        DynamoDbConvertedBy attributeConverterBean =
                getPropertyAnnotation(propertyDescriptor, DynamoDbConvertedBy.class);
        Optional<Class<?>> optionalClass = Optional.ofNullable(attributeConverterBean)
                                                   .map(DynamoDbConvertedBy::value);
        return optionalClass.map(clazz -> (AttributeConverter) newObjectSupplierForClass(clazz, lookup).get());
    }

    /**
     * This method scans all the annotations on a property and looks for a meta-annotation of
     * {@link BeanTableSchemaAttributeTag}. If the meta-annotation is found, it attempts to create
     * an annotation tag based on a standard named static method
     * of the class that tag has been annotated with passing in the original property annotation as an argument.
     */
    private static void addTagsToAttribute(ImmutableAttribute.Builder<?, ?, ?> attributeBuilder,
                                           ImmutablePropertyDescriptor propertyDescriptor) {

        propertyAnnotations(propertyDescriptor).forEach(annotation -> {
            BeanTableSchemaAttributeTag beanTableSchemaAttributeTag =
                annotation.annotationType().getAnnotation(BeanTableSchemaAttributeTag.class);

            if (beanTableSchemaAttributeTag != null) {
                Class<?> tagClass = beanTableSchemaAttributeTag.value();

                Method tagMethod;
                try {
                    tagMethod = tagClass.getDeclaredMethod(ATTRIBUTE_TAG_STATIC_SUPPLIER_NAME,
                                                           annotation.annotationType());
                } catch (NoSuchMethodException e) {
                    throw new RuntimeException(
                        String.format("Could not find a static method named '%s' on class '%s' that returns " +
                                          "an AttributeTag for annotation '%s'", ATTRIBUTE_TAG_STATIC_SUPPLIER_NAME,
                                      tagClass, annotation.annotationType()), e);
                }

                if (!Modifier.isStatic(tagMethod.getModifiers())) {
                    throw new RuntimeException(
                        String.format("Could not find a static method named '%s' on class '%s' that returns " +
                                          "an AttributeTag for annotation '%s'", ATTRIBUTE_TAG_STATIC_SUPPLIER_NAME,
                                      tagClass, annotation.annotationType()));
                }

                StaticAttributeTag staticAttributeTag;
                try {
                    staticAttributeTag = (StaticAttributeTag) tagMethod.invoke(null, annotation);
                } catch (IllegalAccessException | InvocationTargetException e) {
                    throw new RuntimeException(
                        String.format("Could not invoke method to create AttributeTag for annotation '%s' on class " +
                                          "'%s'.", annotation.annotationType(), tagClass), e);
                }

                attributeBuilder.addTag(staticAttributeTag);
            }
        });
    }

    private static <T, R> Supplier<R> newObjectSupplier(ImmutableInfo<T> immutableInfo, Class<R> builderClass,
                                                        MethodHandles.Lookup lookup) {
        if (immutableInfo.staticBuilderMethod().isPresent()) {
            return StaticGetterMethod.create(immutableInfo.staticBuilderMethod().get(), lookup);
        }

        return newObjectSupplierForClass(builderClass, lookup);
    }

    private static <R> Supplier<R> newObjectSupplierForClass(Class<R> clazz, MethodHandles.Lookup lookup) {
        try {
            Constructor<R> constructor = clazz.getConstructor();
            debugLog(clazz, () -> "Constructor: " + constructor);
            return ObjectConstructor.create(clazz, constructor, lookup);
        } catch (NoSuchMethodException e) {
            throw new IllegalArgumentException(
                String.format("Builder class '%s' appears to have no default constructor thus cannot be used with " +
                                  "the ImmutableTableSchema", clazz), e);
        }
    }

    private static <T, R> Function<T, R> getterForProperty(ImmutablePropertyDescriptor propertyDescriptor,
                                                           Class<T> immutableClass,
                                                           MethodHandles.Lookup lookup) {
        Method readMethod = propertyDescriptor.getter();
        debugLog(immutableClass, () -> "Property " + propertyDescriptor.name() + " read method: " + readMethod);
        return BeanAttributeGetter.create(immutableClass, readMethod, lookup);
    }

    private static <T, R> BiConsumer<T, R> setterForProperty(ImmutablePropertyDescriptor propertyDescriptor,
                                                             Class<T> builderClass,
                                                             MethodHandles.Lookup lookup) {
        Method writeMethod = propertyDescriptor.setter();
        debugLog(builderClass, () -> "Property " + propertyDescriptor.name() + " write method: " + writeMethod);
        return BeanAttributeSetter.create(builderClass, writeMethod, lookup);
    }

    private static String attributeNameForProperty(ImmutablePropertyDescriptor propertyDescriptor) {
        DynamoDbAttribute dynamoDbAttribute = getPropertyAnnotation(propertyDescriptor, DynamoDbAttribute.class);
        if (dynamoDbAttribute != null) {
            return dynamoDbAttribute.value();
        }

        return propertyDescriptor.name();
    }

    private static <R extends Annotation> R getPropertyAnnotation(ImmutablePropertyDescriptor propertyDescriptor,
                                                                  Class<R> annotationType) {
        R getterAnnotation = propertyDescriptor.getter().getAnnotation(annotationType);
        R setterAnnotation = propertyDescriptor.setter().getAnnotation(annotationType);

        if (getterAnnotation != null) {
            return getterAnnotation;
        }

        return setterAnnotation;
    }

    private static List<? extends Annotation> propertyAnnotations(ImmutablePropertyDescriptor propertyDescriptor) {
        return Stream.concat(Arrays.stream(propertyDescriptor.getter().getAnnotations()),
                             Arrays.stream(propertyDescriptor.setter().getAnnotations()))
                     .collect(Collectors.toList());
    }

    private static AttributeConfiguration resolveAttributeConfiguration(ImmutablePropertyDescriptor propertyDescriptor) {
        boolean shouldPreserveEmptyObject = getPropertyAnnotation(propertyDescriptor,
                                                                  DynamoDbPreserveEmptyObject.class) != null;

        boolean ignoreNulls = getPropertyAnnotation(propertyDescriptor,
                                                          DynamoDbIgnoreNulls.class) != null;

        return AttributeConfiguration.builder()
                                     .preserveEmptyObject(shouldPreserveEmptyObject)
                                     .ignoreNulls(ignoreNulls)
                                     .build();
    }

    private static void debugLog(Class<?> beanClass, Supplier<String> logMessage) {
        BEAN_LOGGER.debug(() -> beanClass.getTypeName() + " - " + logMessage.get());
    }

    @SdkTestInternalApi
    static void clearSchemaCache() {
        IMMUTABLE_TABLE_SCHEMA_CACHE.clear();
    }
}

