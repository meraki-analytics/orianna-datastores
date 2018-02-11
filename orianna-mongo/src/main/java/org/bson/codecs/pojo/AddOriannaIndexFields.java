package org.bson.codecs.pojo;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import org.bson.BsonDateTime;
import org.bson.BsonInt32;

import com.merakianalytics.orianna.types.common.OriannaException;

public class AddOriannaIndexFields implements Convention {
    private static final Map<Class<?>, Function<Object, BsonInt32>> INCLUDED_DATA_ACCESSORS = new ConcurrentHashMap<>();
    public static final String INCLUDED_DATA_HASH_FIELD_NAME = "_included_data_hash";
    private static final Function<Object, BsonInt32> RETURN_ZERO = (final Object x) -> new BsonInt32(0);
    public static final String UPDATED_FIELD_NAME = "_updated";

    @Override
    public void apply(final ClassModelBuilder<?> classModelBuilder) {
        final PropertyModelBuilder<BsonDateTime> updatedModel =
            PropertyModel.<BsonDateTime> builder().propertyName(UPDATED_FIELD_NAME).writeName(UPDATED_FIELD_NAME)
                .readName(UPDATED_FIELD_NAME).typeData(TypeData.newInstance(null, BsonDateTime.class))
                .propertySerialization((final BsonDateTime value) -> value != null)
                .propertyAccessor(new PropertyAccessor<BsonDateTime>() {
                    @Override
                    public <S> BsonDateTime get(final S instance) {
                        // Get the current time.
                        return new BsonDateTime(ZonedDateTime.now(ZoneOffset.UTC).toEpochSecond() * 1000L);
                    }

                    @Override
                    public <S> void set(final S instance, final BsonDateTime value) {
                        // We don't actually attach it to the object. We just want it to be in MongoDB itself.
                    }
                });

        final PropertyModelBuilder<BsonInt32> includedDataHashModel =
            PropertyModel.<BsonInt32> builder().propertyName(INCLUDED_DATA_HASH_FIELD_NAME).writeName(INCLUDED_DATA_HASH_FIELD_NAME)
                .readName(INCLUDED_DATA_HASH_FIELD_NAME).typeData(TypeData.newInstance(null, BsonInt32.class))
                .propertySerialization((final BsonInt32 value) -> value != null)
                .propertyAccessor(new PropertyAccessor<BsonInt32>() {
                    @Override
                    public <S> BsonInt32 get(final S instance) {
                        Function<Object, BsonInt32> accessor = INCLUDED_DATA_ACCESSORS.get(instance.getClass());
                        if(accessor == null) {
                            synchronized(INCLUDED_DATA_ACCESSORS) {
                                accessor = INCLUDED_DATA_ACCESSORS.get(instance.getClass());
                                if(accessor == null) {
                                    try {
                                        final Method method = instance.getClass().getDeclaredMethod("getIncludedData");

                                        if(method == null) {
                                            accessor = RETURN_ZERO;
                                        } else {
                                            accessor = (final Object x) -> {
                                                try {
                                                    return new BsonInt32(method.invoke(x).hashCode());
                                                } catch(IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
                                                    throw new OriannaException("Failed to call getIncludedData!", e);
                                                }
                                            };
                                        }
                                    } catch(NoSuchMethodException | SecurityException e) {
                                        accessor = RETURN_ZERO;
                                    }

                                    INCLUDED_DATA_ACCESSORS.put(instance.getClass(), accessor);
                                }
                            }
                        }
                        return accessor.apply(instance);
                    }

                    @Override
                    public <S> void set(final S instance, final BsonInt32 value) {
                        // We don't actually attach it to the object. We just want it to be in MongoDB itself.
                    }
                });

        classModelBuilder.addProperty(updatedModel);
        classModelBuilder.addProperty(includedDataHashModel);
    }
}
