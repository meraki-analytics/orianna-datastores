package org.bson.codecs.pojo;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;

import org.bson.BsonDateTime;

public class AddUpdatedTimestamp implements Convention {
    public static final String FIELD_NAME = "_updated";

    @Override
    public void apply(final ClassModelBuilder<?> classModelBuilder) {
        final PropertyModelBuilder<BsonDateTime> updatedModel = PropertyModel.<BsonDateTime> builder().propertyName(FIELD_NAME).writeName(FIELD_NAME)
            .readName(FIELD_NAME).typeData(TypeData.newInstance(null, BsonDateTime.class)).propertySerialization((final BsonDateTime value) -> value != null)
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

        classModelBuilder.addProperty(updatedModel);
    }
}
